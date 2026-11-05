//! Event subscription storage, creation, firing, and delivery.
//!
//! `events_sub:` rows in `kv` are the canonical subscription/event-model
//! contract. The key prefix is `events_sub:` followed by a stable subscription
//! ID. Recognized JSON fields are:
//! - `id`: stable subscription ID, also encoded in the key.
//! - `caller`: agent or external sender that owns the subscription.
//! - `sql`: SQL predicate evaluated against `events_v`.
//! - `params`: optional SQL parameters for parameterized internal subs.
//! - `filters`: original structured filters and internal metadata.
//! - `thread_name`: thread name for delivery-only membership rows.
//! - `on_hit_text`: optional message to send when the subscription fires.
//! - `caller_kind`: frozen sender kind for `on_hit_text` provenance.
//! - `last_id`: cursor for the last event processed by this subscription.
//! - `created`: creation timestamp used for ordering/listing.
//! - `once`: remove the subscription after its first match.
//! - `delivery_only`: internal row used for routing state, not notification.
//! - `auto_thread_member`: delivery-only thread membership marker.
//!
//! Subscription kinds stored under this prefix are filter subs, SQL subs,
//! request watches (`reqwatch-*`), delivery-only thread members, and collision
//! subs. New code should not write `events_sub:` rows outside this module.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use rusqlite::{OptionalExtension, params};
use serde_json::json;

use super::HcomDb;
use crate::core::filters::{FILE_WRITE_CONTEXTS, build_sql_from_flags, collision_window_sql};
use crate::messages::{InstanceInfo, MessageScope, ScopeResult, compute_scope, resolve_targets};
use crate::shared::constants::extract_mentions;

fn subscription_is_delivery_only(sub: &serde_json::Value) -> bool {
    match sub.get("delivery_only") {
        Some(serde_json::Value::Bool(flag)) => *flag,
        Some(serde_json::Value::Number(n)) => n.as_i64() == Some(1),
        Some(serde_json::Value::String(s)) => s.eq_ignore_ascii_case("true") || s == "1",
        _ => false,
    }
}

/// Stable subscription ID for automatic thread membership rows.
pub(crate) fn thread_membership_sub_id(thread: &str, member: &str) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(format!("thread-member:{thread}:{member}").as_bytes());
    let hash = hasher.finalize();
    let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
    format!("sub-{}", &hex[..8])
}

/// Outcome of a subscription insert attempt.
pub(crate) enum SubCreateOutcome {
    Created { id: String, final_sql: String },
    AlreadyExists { id: String },
}

/// Build and insert a filter-based subscription row into `kv`.
pub(crate) fn create_filter_subscription(
    db: &HcomDb,
    filters: &HashMap<String, Vec<String>>,
    sql_parts: &[String],
    caller: &str,
    once: bool,
    on_hit: Option<&str>,
) -> Result<SubCreateOutcome, String> {
    let mut sql = match build_sql_from_flags(filters) {
        Ok(s) if !s.is_empty() => s,
        Ok(_) => return Err("No valid filters provided".to_string()),
        Err(e) => return Err(format!("Filter error: {e}")),
    };

    if !sql_parts.is_empty() {
        let manual_sql = sql_parts.join(" ").replace("\\!", "!");
        if let Err(e) = db.conn().execute(
            &format!("SELECT 1 FROM events_v WHERE ({manual_sql}) LIMIT 0"),
            [],
        ) {
            return Err(format!("Invalid SQL: {e}"));
        }
        sql = format!("({sql}) AND ({manual_sql})");
    }

    if filters.contains_key("collision") {
        let self_relevance = collision_self_relevance_sql(caller);
        sql = format!("({sql}) AND {self_relevance}");
    }

    let id_source = format!(
        "{}:{}:{}:{}:{}",
        caller,
        serde_json::to_string(filters).unwrap_or_default(),
        sql,
        once,
        on_hit.unwrap_or(""),
    );
    let hash = sha256_hash(&id_source);
    let sub_id = format!("sub-{}", &hash[..8]);
    let sub_key = format!("events_sub:{sub_id}");

    if db.kv_get(&sub_key).ok().flatten().is_some() {
        return Ok(SubCreateOutcome::AlreadyExists { id: sub_id });
    }

    let now = crate::shared::time::now_epoch_f64();
    let last_id = db.get_last_event_id();

    let mut sub_data = json!({
        "id": sub_id,
        "caller": caller,
        "filters": filters,
        "sql": sql,
        "created": now,
        "last_id": last_id,
        "once": once,
    });
    if let Some(text) = on_hit {
        sub_data["on_hit_text"] = json!(text);
        sub_data["caller_kind"] = json!(resolve_caller_kind(db, caller));
    }

    let _ = db.kv_set(&sub_key, Some(&sub_data.to_string()));

    Ok(SubCreateOutcome::Created {
        id: sub_id,
        final_sql: sql,
    })
}

/// Build and insert a raw-SQL subscription row into `kv`.
pub(crate) fn build_and_insert_sql_subscription(
    db: &HcomDb,
    sql_parts: &[String],
    caller: &str,
    once: bool,
    on_hit: Option<&str>,
) -> Result<SubCreateOutcome, String> {
    let sql = sql_parts.join(" ").replace("\\!", "!");

    if let Err(e) = db
        .conn()
        .execute(&format!("SELECT 1 FROM events_v WHERE ({sql}) LIMIT 0"), [])
    {
        return Err(format!("Invalid SQL: {e}"));
    }

    let hash = sha256_hash(&format!("{caller}{sql}{once}{}", on_hit.unwrap_or("")));
    let sub_id = format!("sub-{}", &hash[..8]);
    let sub_key = format!("events_sub:{sub_id}");

    if db.kv_get(&sub_key).ok().flatten().is_some() {
        return Ok(SubCreateOutcome::AlreadyExists { id: sub_id });
    }

    let now = crate::shared::time::now_epoch_f64();
    let last_id = db.get_last_event_id();

    let mut sub_data = json!({
        "id": sub_id,
        "sql": sql,
        "caller": caller,
        "once": once,
        "last_id": last_id,
        "created": now,
    });
    if let Some(text) = on_hit {
        sub_data["on_hit_text"] = json!(text);
        sub_data["caller_kind"] = json!(resolve_caller_kind(db, caller));
    }

    let _ = db.kv_set(&sub_key, Some(&sub_data.to_string()));

    Ok(SubCreateOutcome::Created {
        id: sub_id,
        final_sql: sql,
    })
}

pub(crate) use super::reqwatch_policy::AGY_REQWATCH_IDLE_GRACE_SEC;

fn instance_tool(db: &HcomDb, name: &str) -> String {
    db.conn()
        .query_row(
            "SELECT COALESCE(tool, '') FROM instances WHERE name = ?",
            params![name],
            |row| row.get(0),
        )
        .unwrap_or_default()
}

fn reqwatch_reply_exists(db: &HcomDb, request_id: i64, target: &str, sub_caller: &str) -> bool {
    if sub_caller.is_empty() {
        return false;
    }
    db.conn()
        .query_row(
            "SELECT 1 FROM events_v WHERE id > ? AND type = 'message' \
             AND msg_from = ? AND (\
               (msg_scope = 'mentions' AND EXISTS (\
                  SELECT 1 FROM json_each(msg_delivered_to) WHERE value = ?\
                )) \
               OR json_extract(data, '$.reply_to_local') = ? \
             )",
            params![request_id, target, sub_caller, request_id],
            |_| Ok(true),
        )
        .unwrap_or(false)
}

fn kv_store_sub(db: &HcomDb, key: &str, sub: &serde_json::Value) {
    match serde_json::to_string(sub) {
        Ok(json) => {
            if let Err(e) = db.kv_set(key, Some(&json)) {
                crate::log::log_error("db", "reqwatch.kv_set", &format!("{e}"));
            }
        }
        Err(e) => crate::log::log_error("db", "reqwatch.serialize", &format!("{e}")),
    }
}

/// Clear agy grace timers when the target is working again (deliver/tool/active).
fn clear_agy_reqwatch_idle_grace(db: &HcomDb, target: &str, only: Option<&HashSet<String>>) {
    for (key, sub, filters) in load_reqwatch_subs(db, only) {
        if filters.get("target_tool").and_then(|v| v.as_str()) != Some("antigravity") {
            continue;
        }
        if filters.get("target").and_then(|v| v.as_str()) != Some(target) {
            continue;
        }
        if sub.get("idle_grace_until").is_none() {
            continue;
        }
        let mut sub_mut = sub.clone();
        if let Some(obj) = sub_mut.as_object_mut() {
            obj.remove("idle_grace_until");
            obj.remove("idle_grace_event_id");
            kv_store_sub(db, &key, &sub_mut);
        }
    }
}

/// Fire Antigravity request watches whose idle grace elapsed while no matching
/// event arrived. The conditional delete is the claim: concurrent sweepers can
/// observe the same row, but only one can remove it and emit the one-shot notice.
fn sweep_expired_reqwatch_graces(db: &HcomDb, now: f64) {
    for (key, sub, filters) in load_reqwatch_subs(db, None) {
        if filters.get("target_tool").and_then(|v| v.as_str()) != Some("antigravity")
            || !super::reqwatch_policy::idle_grace_expired(&sub, now)
        {
            continue;
        }

        let request_id = filters
            .get("request_id")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let target = filters.get("target").and_then(|v| v.as_str()).unwrap_or("");
        let caller = sub.get("caller").and_then(|v| v.as_str()).unwrap_or("");
        if request_id <= 0 || target.is_empty() || caller.is_empty() {
            continue;
        }

        if reqwatch_reply_exists(db, request_id, target, caller) {
            let _ = db.kv_set(&key, None);
            continue;
        }

        let candidate_event_id = sub
            .get("idle_grace_event_id")
            .and_then(|v| v.as_i64())
            .or_else(|| sub.get("last_id").and_then(|v| v.as_i64()))
            .unwrap_or(0);

        let claimed = db
            .conn()
            .execute(
                "DELETE FROM kv
                 WHERE key = ?1
                   AND CAST(json_extract(value, '$.idle_grace_until') AS REAL) <= ?2
                   AND json_extract(value, '$.filters.target_tool') = 'antigravity'
                   AND EXISTS (
                       SELECT 1 FROM instances
                       WHERE name = ?3 AND status = 'listening' AND last_event_id >= ?4
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM events_v
                       WHERE id > ?4 AND type = 'message' AND msg_from = ?3
                         AND (
                             (msg_scope = 'mentions' AND EXISTS (
                                 SELECT 1 FROM json_each(msg_delivered_to) WHERE value = ?5
                             ))
                             OR json_extract(data, '$.reply_to_local') = ?4
                         )
                   )",
                params![key, now, target, request_id, caller],
            )
            .unwrap_or(0);
        if claimed != 1 {
            continue;
        }

        let sub_id = sub
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or(key.as_str());
        let notification = format_sub_notification(
            db,
            sub_id,
            candidate_event_id,
            "status",
            target,
            &serde_json::json!({"status": "listening"}),
            Some(&filters),
        );
        let _ = send_sub_notification(db, caller, &notification);
    }
}

/// Create request-watch subscriptions for each recipient.
pub(crate) fn create_request_watches(
    db: &HcomDb,
    sender: &str,
    request_event_id: i64,
    recipients: &[String],
) {
    let last_id = db.get_last_event_id();
    let now = crate::shared::time::now_epoch_f64();

    for recipient in recipients {
        let sub_id = format!("reqwatch-{request_event_id}-{recipient}");
        let sub_key = format!("events_sub:{sub_id}");
        let target_tool = instance_tool(db, recipient);

        let sql = "(type='status' AND instance=? AND status_val='listening') OR (type='life' AND instance=? AND life_action='stopped')";

        let sub_data = serde_json::json!({
            "id": sub_id,
            "caller": sender,
            "sql": sql,
            "params": [recipient, recipient],
            "filters": {
                "request_watch": true,
                "request_id": request_event_id,
                "target": recipient,
                "target_tool": target_tool,
            },
            "once": true,
            "last_id": last_id,
            "created": now,
        });

        kv_store_sub(db, &sub_key, &sub_data);
    }
}

/// Remove all event subscriptions owned by an instance.
pub(crate) fn cleanup_subscriptions(db: &HcomDb, name: &str) -> Result<u32> {
    let deleted = db.conn.execute(
        "DELETE FROM kv
         WHERE key LIKE 'events_sub:%'
           AND json_extract(value, '$.caller') = ?
           AND COALESCE(json_extract(value, '$.delivery_only'), 0) != 1",
        params![name],
    )?;
    Ok(deleted as u32)
}

/// Every subscription row an instance owns, thread memberships included, for
/// its stop snapshot. Read inside the stop transaction, before deletion.
///
/// Live matching runs as each event is logged but moves `last_id` only on a
/// hit, so a live sub has already seen every event up to now. Save it with
/// the newest event id as its cursor: resume must replay only the stopped
/// gap, not re-run older events with hindsight (a collision sub would match
/// an edit that only collided with a later one).
fn owned_subscriptions(db: &HcomDb, name: &str) -> Result<Vec<serde_json::Value>> {
    let newest = db.get_last_event_id();
    let mut stmt = db.conn.prepare(
        "SELECT value FROM kv
         WHERE key LIKE 'events_sub:%'
           AND json_extract(value, '$.caller') = ?",
    )?;
    let rows = stmt.query_map(params![name], |row| row.get::<_, String>(0))?;
    Ok(rows
        .filter_map(|r| r.ok())
        .filter_map(|v| serde_json::from_str::<serde_json::Value>(&v).ok())
        .map(|mut sub| {
            let last_id = sub.get("last_id").and_then(|v| v.as_i64()).unwrap_or(0);
            sub["last_id"] = json!(last_id.max(newest));
            sub
        })
        .collect())
}

/// Subscriptions a stop snapshot should keep for `hcom r`: those the row owns
/// now, plus any an earlier soft stop of the same row (same `created_at`)
/// saved and deleted while keeping the row. The live row wins on conflict.
pub(crate) fn subscriptions_to_save(
    db: &HcomDb,
    name: &str,
    created_at: f64,
) -> Result<Vec<serde_json::Value>> {
    let owned = owned_subscriptions(db, name)?;
    let last_stop: Option<String> = db
        .conn
        .query_row(
            "SELECT data FROM events
             WHERE type = 'life' AND instance = ?
               AND json_extract(data, '$.action') = 'stopped'
             ORDER BY id DESC LIMIT 1",
            params![name],
            |row| row.get(0),
        )
        .optional()?;
    let soft_saved = last_stop
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .filter(|d| d["snapshot"]["created_at"].as_f64() == Some(created_at))
        .and_then(|d| d["snapshot"]["subscriptions"].as_array().cloned())
        .unwrap_or_default();
    let owned_ids: HashSet<String> = owned
        .iter()
        .filter_map(|s| s.get("id").and_then(|v| v.as_str()).map(String::from))
        .collect();
    let mut merged = owned;
    merged.extend(soft_saved.into_iter().filter(|s| {
        s.get("id")
            .and_then(|v| v.as_str())
            .is_some_and(|id| !owned_ids.contains(id))
    }));
    Ok(merged)
}

/// Most events a resume replays against restored subscriptions; a longer gap
/// replays only its newest events.
const RESUME_REPLAY_LIMIT: i64 = 1000;

/// Restore the subscriptions saved in `name`'s latest stop snapshot, then
/// replay the events it missed while stopped against them, so a match that
/// happened during the gap still notifies. Only for a tracked resume of the
/// same identity: a reused name must not inherit them. Returns the number
/// of subscriptions restored.
pub(crate) fn restore_stopped_subscriptions(db: &HcomDb, name: &str) -> Result<u32> {
    let saved: Option<String> = db
        .conn
        .query_row(
            "SELECT json_extract(data, '$.snapshot.subscriptions') FROM events
             WHERE type = 'life' AND instance = ?
               AND json_extract(data, '$.action') = 'stopped'
             ORDER BY id DESC LIMIT 1",
            params![name],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let Some(serde_json::Value::Array(subs)) = saved.and_then(|s| serde_json::from_str(&s).ok())
    else {
        return Ok(0);
    };

    // Each restored sub's saved cursor: an event at or before it predates the
    // sub's position and must neither match nor transition it.
    let mut cursors: HashMap<String, i64> = HashMap::new();
    for sub in &subs {
        let Some(id) = sub.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        if sub.get("caller").and_then(|v| v.as_str()) != Some(name) {
            continue;
        }
        let key = format!("events_sub:{id}");
        kv_store_sub(db, &key, sub);
        if !subscription_is_delivery_only(sub) {
            cursors.insert(
                key,
                sub.get("last_id").and_then(|v| v.as_i64()).unwrap_or(0),
            );
        }
    }
    let restored = subs.len() as u32;
    let Some(&replay_from) = cursors.values().min() else {
        return Ok(restored);
    };

    let mut stmt = db.conn.prepare(
        "SELECT id, type, instance, data FROM
           (SELECT id, type, instance, data FROM events WHERE id > ? ORDER BY id DESC LIMIT ?)
         ORDER BY id",
    )?;
    let events: Vec<(i64, String, String, String)> = stmt
        .query_map(params![replay_from, RESUME_REPLAY_LIMIT], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .filter_map(|r| r.ok())
        .collect();
    // An on-hit reaction logs a new event whose live processing would move
    // restored cursors past historical events not yet replayed. Hold
    // reactions until the historical batch is drained.
    let mut replay = ReplayState {
        predicates: ReplayPredicates::new(&db.conn),
        reactions: Vec::new(),
        rows: None,
    };
    for (event_id, event_type, instance, data) in events {
        let data: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();
        if is_system_feedback(&event_type, &instance, &data) {
            continue;
        }
        let eligible: HashSet<String> = cursors
            .iter()
            .filter(|(_, cursor)| **cursor < event_id)
            .map(|(key, _)| key.clone())
            .collect();
        apply_request_watch_transitions(db, &event_type, &instance, &data, Some(&eligible));
        fire_matching_subscriptions(
            db,
            event_id,
            &event_type,
            &instance,
            &data,
            Some(&eligible),
            Some(&mut replay),
        );
    }
    let reactions = std::mem::take(&mut replay.reactions);
    drop(replay); // release all reset statements before reactions recurse
    for reaction in reactions {
        reaction.send(db);
    }
    Ok(restored)
}

/// Remove delivery-only thread memberships for an instance name reuse.
pub(crate) fn cleanup_thread_memberships_for_name_reuse(db: &HcomDb, name: &str) -> Result<u32> {
    let deleted = db.conn.execute(
        "DELETE FROM kv
         WHERE key LIKE 'events_sub:%'
           AND json_extract(value, '$.caller') = ?
           AND json_extract(value, '$.auto_thread_member') = 1
           AND COALESCE(json_extract(value, '$.delivery_only'), 0) = 1",
        params![name],
    )?;
    Ok(deleted as u32)
}

/// Return active members of a thread in join order.
pub(crate) fn get_thread_members(db: &HcomDb, thread: &str) -> Vec<String> {
    let active_instances: HashSet<String> = db
        .conn()
        .prepare("SELECT name FROM instances")
        .ok()
        .map(|mut stmt| {
            stmt.query_map([], |row| row.get::<_, String>(0))
                .ok()
                .into_iter()
                .flatten()
                .filter_map(|r| r.ok())
                .collect()
        })
        .unwrap_or_default();

    let rows: Vec<String> = db
        .conn()
        .prepare(
            "SELECT value FROM kv
             WHERE key LIKE 'events_sub:%'
               AND json_extract(value, '$.auto_thread_member') = 1
               AND json_extract(value, '$.thread_name') = ?
             ORDER BY json_extract(value, '$.created') ASC, key ASC",
        )
        .ok()
        .map(|mut stmt| {
            stmt.query_map(rusqlite::params![thread], |row| row.get::<_, String>(0))
                .ok()
                .into_iter()
                .flatten()
                .filter_map(|r| r.ok())
                .collect()
        })
        .unwrap_or_default();

    let mut members = Vec::new();
    let mut seen = HashSet::new();
    for value in rows {
        let caller = serde_json::from_str::<serde_json::Value>(&value)
            .ok()
            .and_then(|sub| sub.get("caller").and_then(|v| v.as_str()).map(String::from));
        if let Some(caller) = caller
            && active_instances.contains(&caller)
            && seen.insert(caller.clone())
        {
            members.push(caller);
        }
    }
    members
}

/// Upsert memberships for recipients of a thread message.
pub(crate) fn add_thread_memberships(
    db: &HcomDb,
    thread: &str,
    sender: Option<&str>,
    recipients: &[String],
) {
    let mut members = recipients.to_vec();
    if let Some(sender) = sender {
        members.push(sender.to_string());
    }

    let now = crate::shared::time::now_epoch_f64();
    let last_id = db.get_last_event_id();
    let mut seen = HashSet::new();
    for (idx, member) in members.into_iter().enumerate() {
        if !seen.insert(member.clone()) {
            continue;
        }
        let sub_id = thread_membership_sub_id(thread, &member);
        let key = format!("events_sub:{sub_id}");
        let data = serde_json::json!({
            "id": sub_id,
            "caller": member,
            "thread_name": thread,
            "auto_thread_member": true,
            "delivery_only": true,
            "sql": "0",
            "created": now + (idx as f64 * 0.000001),
            "last_id": last_id,
            "once": false,
        });
        let _ = db.kv_set(&key, Some(&data.to_string()));
    }
}

/// Recursion guard: events that could cause notification loops.
fn is_system_feedback(event_type: &str, instance: &str, data: &serde_json::Value) -> bool {
    if instance.starts_with("sys_") {
        return true;
    }
    if event_type == "message" {
        let sender = data.get("from").and_then(|v| v.as_str()).unwrap_or("");
        let sender_kind = data
            .get("sender_kind")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if sender == "[hcom-events]" || sender_kind == "system" {
            return true;
        }
    }
    false
}

/// Check subscriptions and send matching notifications.
/// Called inline from log_event(). Errors logged, never propagated.
pub(crate) fn process_logged_event(
    db: &HcomDb,
    event_id: i64,
    event_type: &str,
    instance: &str,
    data: &serde_json::Value,
) {
    if is_system_feedback(event_type, instance, data) {
        return;
    }

    apply_request_watch_transitions(db, event_type, instance, data, None);
    fire_matching_subscriptions(db, event_id, event_type, instance, data, None, None);

    // A grace expiry is not itself an event. Sweep after handling the current
    // event so replies, active/blocked transitions, and stop events win first.
    sweep_expired_reqwatch_graces(db, crate::shared::time::now_epoch_f64());
}

/// Request-watch state an event changes before matching: replies cancel
/// watches, and agy grace resets. Limited to `only` (subscription keys).
fn apply_request_watch_transitions(
    db: &HcomDb,
    event_type: &str,
    instance: &str,
    data: &serde_json::Value,
    only: Option<&HashSet<String>>,
) {
    if event_type == "message" {
        let msg_sender = data.get("from").and_then(|v| v.as_str()).unwrap_or("");
        let reply_to_id = data.get("reply_to_local").and_then(|v| v.as_i64());

        if let Some("mentions") = data.get("scope").and_then(|v| v.as_str()) {
            let msg_delivered_to: Vec<String> = data
                .get("delivered_to")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            if !msg_sender.is_empty() && !msg_delivered_to.is_empty() {
                cancel_request_watches_by_flow(
                    db,
                    msg_sender,
                    &msg_delivered_to,
                    reply_to_id,
                    only,
                );
            }
        }

        if let Some(rid) = reply_to_id
            && !msg_sender.is_empty()
        {
            cancel_request_watches_by_reply_id(db, msg_sender, rid, only);
        }
    }

    // agy: turn-end `listening` is normal; reset reqwatch grace when target is active again.
    if event_type == "status" {
        let status = data.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status == "active" || status == "blocked" {
            clear_agy_reqwatch_idle_grace(db, instance, only);
        }
    }
}

/// An `--on-hit` message a subscription sends when it fires.
struct OnHitReaction {
    caller: String,
    caller_kind: String,
    text: String,
    chain: Vec<String>,
}

impl OnHitReaction {
    fn send(&self, db: &HcomDb) {
        if let Err(e) =
            send_reaction_message(db, &self.caller, &self.caller_kind, &self.text, &self.chain)
        {
            crate::log::log_error("db", "check_event_subscriptions.on_hit", &format!("{e}"));
        }
    }
}

// Bound both retained statement count and SQL text. The cache lives for one
// historical replay only; it never caches mutable subscription records/results.
const REPLAY_PREDICATE_LIMIT: usize = 256;
const REPLAY_PREDICATE_SQL_BYTES: usize = 256 * 1024;

struct ReplayPredicates<'conn> {
    conn: &'conn rusqlite::Connection,
    statements: HashMap<String, rusqlite::Statement<'conn>>,
    sql_bytes: usize,
}

impl<'conn> ReplayPredicates<'conn> {
    fn new(conn: &'conn rusqlite::Connection) -> Self {
        Self {
            conn,
            statements: HashMap::new(),
            sql_bytes: 0,
        }
    }

    fn matches(&mut self, query: &str, params: impl rusqlite::Params) -> bool {
        if let Some(statement) = self.statements.get_mut(query) {
            return statement.query_row(params, |_| Ok(())).is_ok();
        }
        let Ok(mut statement) = self.conn.prepare(query) else {
            return false;
        };
        let matched = statement.query_row(params, |_| Ok(())).is_ok();
        // Retain admitted entries rather than evicting on every overflow miss:
        // a replay cycling through >limit predicates would otherwise thrash.
        if self.statements.len() < REPLAY_PREDICATE_LIMIT
            && query.len() <= REPLAY_PREDICATE_SQL_BYTES.saturating_sub(self.sql_bytes)
        {
            self.sql_bytes += query.len();
            self.statements.insert(query.to_string(), statement);
        }
        matched
    }
}

struct ReplayState<'conn> {
    predicates: ReplayPredicates<'conn>,
    reactions: Vec<OnHitReaction>,
    rows: Option<ReplayRows>,
}

// Reuse parsed rows only while this connection owns the replay transaction and
// no write (including a trigger, cursor update or request-watch transition) has
// happened. Other connections cannot write during that transaction. Overflow
// falls back to fresh reads; these bounds cover retained rows/text, not the
// allocator overhead of parsed JSON or compiled predicates.
const REPLAY_ROW_LIMIT: usize = 1024;
const REPLAY_ROW_BYTES: usize = 1024 * 1024;

struct SubscriptionRow {
    key: String,
    value: String,
    sub: Option<serde_json::Value>,
}

struct ReplayRows {
    changes: u64,
    rows: std::sync::Arc<Vec<SubscriptionRow>>,
}

fn subscription_rows(
    db: &HcomDb,
    only: Option<&HashSet<String>>,
    replay: Option<&mut ReplayState<'_>>,
) -> std::sync::Arc<Vec<SubscriptionRow>> {
    let changes = db.conn.total_changes();
    let in_transaction = !db.conn.is_autocommit();
    if in_transaction
        && let Some(cached) = replay.as_ref().and_then(|state| state.rows.as_ref())
        && cached.changes == changes
    {
        return std::sync::Arc::clone(&cached.rows);
    }
    let mut bytes = 0usize;
    let mut cacheable = true;
    let loaded = (|| -> rusqlite::Result<Vec<SubscriptionRow>> {
        let mut stmt = db.conn.prepare_cached(
            "SELECT key, value FROM kv
         WHERE key LIKE 'events_sub:%'
           AND COALESCE(json_extract(value, '$.delivery_only'), 0) != 1
           AND COALESCE(json_extract(value, '$.delivery_only'), 'false') != 'true'",
        )?;
        let iter = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut rows = Vec::new();
        for row in iter {
            match row {
                Ok((key, value)) => {
                    bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
                    rows.push(SubscriptionRow {
                        key,
                        value,
                        sub: None,
                    });
                }
                Err(_) => cacheable = false,
            }
        }
        Ok(rows)
    })();
    let mut rows = match loaded {
        Ok(rows) => rows,
        Err(_) => {
            cacheable = false;
            Vec::new()
        }
    };
    let retain = replay.is_some()
        && cacheable
        && in_transaction
        && rows.len() <= REPLAY_ROW_LIMIT
        && bytes <= REPLAY_ROW_BYTES;
    if retain {
        // Live calls and overflow keep the original one-row-at-a-time parsing.
        // A saved cursor may make a row eligible later in this replay; leave it
        // unparsed now and evaluate it from its raw value when that happens.
        for row in &mut rows {
            if only.is_none_or(|keys| keys.contains(&row.key)) {
                row.sub = serde_json::from_str(&row.value).ok();
            }
        }
    }
    let rows = std::sync::Arc::new(rows);
    if let Some(replay) = replay {
        replay.rows = retain.then(|| ReplayRows {
            changes,
            rows: std::sync::Arc::clone(&rows),
        });
    }
    rows
}

/// Fire subscriptions matching one logged event, optionally limited to `only`
/// (subscription keys). Advances cursors and removes `--once` hits. On-hit
/// reactions are sent now, or queued in `replay` when given.
fn fire_matching_subscriptions<'conn>(
    db: &'conn HcomDb,
    event_id: i64,
    event_type: &str,
    instance: &str,
    data: &serde_json::Value,
    only: Option<&HashSet<String>>,
    mut replay: Option<&mut ReplayState<'conn>>,
) {
    let rows = subscription_rows(db, only, replay.as_deref_mut());

    if rows.is_empty() {
        return;
    }

    for SubscriptionRow { key, value, sub } in rows.iter() {
        if only.is_some_and(|keys| !keys.contains(key)) {
            continue;
        }
        let mut sub = match sub {
            Some(sub) => std::borrow::Cow::Borrowed(sub),
            None => match serde_json::from_str::<serde_json::Value>(value) {
                Ok(sub) => std::borrow::Cow::Owned(sub),
                Err(_) => continue,
            },
        };
        if subscription_is_delivery_only(&sub) {
            continue;
        }
        if let Some(sql) = sub
            .get("filters")
            .and_then(|f| f.get("collision"))
            .and(sub.get("sql"))
            .and_then(|v| v.as_str())
            .and_then(upgrade_collision_sql)
        {
            // Only the sql field, and only if no other process rewrote the
            // record since it was read (it may have advanced last_id).
            if let Err(e) = db.conn.execute(
                "UPDATE kv SET value = json_set(value, '$.sql', ?1) WHERE key = ?2 AND value = ?3",
                params![sql, key, value],
            ) {
                crate::log::log_error("db", "collision_sql_upgrade", &format!("{e}"));
            }
            sub.to_mut()["sql"] = json!(sql);
        }
        let sub_id = sub
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or(key.as_str());

        // Carry the reaction chain so a subscription never reacts to output it
        // caused, directly or through other subscriptions. Independent
        // subscriptions still react.
        let reaction_chain: Vec<String> = data
            .get("reaction_subscriptions")
            .and_then(|v| v.as_array())
            .map(|ids| {
                ids.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if reaction_chain.iter().any(|id| id == sub_id) {
            continue;
        }

        let last_id = sub.get("last_id").and_then(|v| v.as_i64()).unwrap_or(0);
        if event_id <= last_id {
            continue;
        }

        // The collision clause needs a file-write status event; checking that
        // here skips one SQL query per agent for every other event.
        if sub
            .get("filters")
            .and_then(|f| f.get("collision"))
            .is_some()
            && !(event_type == "status"
                && data
                    .get("context")
                    .and_then(|v| v.as_str())
                    .is_some_and(crate::core::filters::is_file_write_context)
                && data
                    .get("detail")
                    .and_then(|v| v.as_str())
                    .is_some_and(|d| !d.is_empty()))
        {
            continue;
        }

        let sql = sub.get("sql").and_then(|v| v.as_str()).unwrap_or("");
        if !sql.is_empty() {
            let filter_query = format!("SELECT 1 FROM events_v WHERE id = ? AND ({})", sql);
            let stored_params: Vec<String> = sub
                .get("params")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            let matched = if stored_params.is_empty() {
                match replay.as_deref_mut() {
                    Some(replay) => replay.predicates.matches(&filter_query, params![event_id]),
                    None => db
                        .conn
                        .query_row(&filter_query, params![event_id], |_| Ok(()))
                        .is_ok(),
                }
            } else {
                let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(event_id)];
                for p in &stored_params {
                    all_params.push(Box::new(p.clone()));
                }
                let refs: Vec<&dyn rusqlite::types::ToSql> =
                    all_params.iter().map(|p| p.as_ref()).collect();
                match replay.as_deref_mut() {
                    Some(replay) => replay.predicates.matches(&filter_query, refs.as_slice()),
                    None => db
                        .conn
                        .query_row(&filter_query, refs.as_slice(), |_| Ok(()))
                        .is_ok(),
                }
            };

            if !matched {
                continue;
            }
        }

        let sub_filters = sub
            .get("filters")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        if sub_filters.get("request_watch").is_some() {
            let request_id = sub_filters
                .get("request_id")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let target = sub_filters
                .get("target")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let sub_caller = sub.get("caller").and_then(|v| v.as_str()).unwrap_or("");
            if request_id > 0 && !target.is_empty() {
                let waterline: i64 = db
                    .conn
                    .query_row(
                        "SELECT last_event_id FROM instances WHERE name = ?",
                        params![target],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);
                if waterline < request_id {
                    let mut sub_mut = sub.as_ref().clone();
                    sub_mut["last_id"] = serde_json::json!(event_id);
                    kv_store_sub(db, key, &sub_mut);
                    continue;
                }

                if reqwatch_reply_exists(db, request_id, target, sub_caller) {
                    if let Err(e) = db.kv_set(key, None) {
                        crate::log::log_error(
                            "db",
                            "check_event_subscriptions.kv_set_cleanup",
                            &format!("{e}"),
                        );
                    }
                    continue;
                }

                let target_tool = sub_filters
                    .get("target_tool")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let now = crate::shared::time::now_epoch_f64();
                match super::reqwatch_policy::reqwatch_notify_decision(
                    target_tool,
                    event_type,
                    data,
                    &sub,
                    now,
                ) {
                    super::reqwatch_policy::ReqwatchNotifyDecision::Skip => continue,
                    super::reqwatch_policy::ReqwatchNotifyDecision::Defer {
                        set_grace_if_absent,
                    } => {
                        let mut sub_mut = sub.as_ref().clone();
                        sub_mut["last_id"] = serde_json::json!(event_id);
                        if set_grace_if_absent {
                            sub_mut["idle_grace_until"] =
                                serde_json::json!(now + AGY_REQWATCH_IDLE_GRACE_SEC);
                            sub_mut["idle_grace_event_id"] = serde_json::json!(event_id);
                        }
                        kv_store_sub(db, key, &sub_mut);
                        continue;
                    }
                    super::reqwatch_policy::ReqwatchNotifyDecision::Proceed => {}
                }
            }
        }

        let still_exists: bool = db
            .conn
            .query_row("SELECT 1 FROM kv WHERE key = ?", params![key], |_| Ok(true))
            .unwrap_or(false);
        if !still_exists {
            continue;
        }

        let caller = sub.get("caller").and_then(|v| v.as_str()).unwrap_or("");
        if caller.is_empty() {
            continue;
        }

        let filters_opt = sub.get("filters");
        let notification = format_sub_notification(
            db,
            sub_id,
            event_id,
            event_type,
            instance,
            data,
            filters_opt,
        );
        let _ = send_sub_notification(db, caller, &notification);

        if let Some(on_hit_text) = sub.get("on_hit_text").and_then(|v| v.as_str()) {
            let caller_kind = sub
                .get("caller_kind")
                .and_then(|v| v.as_str())
                .unwrap_or("external");
            let mut chain = reaction_chain.clone();
            chain.push(sub_id.to_string());
            let reaction = OnHitReaction {
                caller: caller.to_string(),
                caller_kind: caller_kind.to_string(),
                text: on_hit_text.to_string(),
                chain,
            };
            match replay.as_deref_mut() {
                Some(replay) => replay.reactions.push(reaction),
                None => reaction.send(db),
            }
        }

        if sub.get("once").and_then(|v| v.as_bool()).unwrap_or(false) {
            if let Err(e) = db.kv_set(key, None) {
                crate::log::log_error(
                    "db",
                    "check_event_subscriptions.kv_set_once",
                    &format!("{e}"),
                );
            }
        } else {
            let mut sub_mut = sub.as_ref().clone();
            sub_mut["last_id"] = serde_json::json!(event_id);
            match serde_json::to_string(&sub_mut) {
                Ok(json) => {
                    if let Err(e) = db.kv_set(key, Some(&json)) {
                        crate::log::log_error(
                            "db",
                            "check_event_subscriptions.kv_set_cursor",
                            &format!("{e}"),
                        );
                    }
                }
                Err(e) => {
                    crate::log::log_error(
                        "db",
                        "check_event_subscriptions.serialize_cursor",
                        &format!("{e}"),
                    );
                }
            }
        }
    }
}

/// Load all reqwatch subscriptions as (key, parsed_sub, filters) tuples.
pub(crate) fn load_reqwatch_subs(
    db: &HcomDb,
    only: Option<&HashSet<String>>,
) -> Vec<(String, serde_json::Value, serde_json::Value)> {
    let rows: Vec<(String, String)> = match db
        .conn
        .prepare_cached("SELECT key, value FROM kv WHERE key LIKE 'events_sub:reqwatch-%'")
    {
        Ok(mut stmt) => stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .ok()
            .map(|iter| iter.filter_map(|r| r.ok()).collect())
            .unwrap_or_default(),
        Err(_) => return vec![],
    };

    rows.into_iter()
        .filter(|(key, _)| only.is_none_or(|keys| keys.contains(key)))
        .filter_map(|(key, value)| {
            let sub: serde_json::Value = serde_json::from_str(&value).ok()?;
            let filters = sub.get("filters")?.clone();
            Some((key, sub, filters))
        })
        .collect()
}

/// Cancel request-watch subs when watched target messages the requester.
pub(crate) fn cancel_request_watches_by_flow(
    db: &HcomDb,
    sender: &str,
    delivered_to: &[String],
    reply_to_id: Option<i64>,
    only: Option<&HashSet<String>>,
) {
    for (key, sub, filters) in &load_reqwatch_subs(db, only) {
        let target = filters.get("target").and_then(|v| v.as_str()).unwrap_or("");
        let sub_caller = sub.get("caller").and_then(|v| v.as_str()).unwrap_or("");

        if target == sender && delivered_to.iter().any(|d| d == sub_caller) {
            if let Some(rid) = reply_to_id {
                let req_id = filters
                    .get("request_id")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                if req_id != rid {
                    continue;
                }
            }
            if let Err(e) = db.kv_set(key, None) {
                crate::log::log_error(
                    "db",
                    "cancel_request_watches_by_flow.kv_set",
                    &format!("{e}"),
                );
            }
        }
    }
}

/// Cancel request-watch subs by explicit reply_to match.
pub(crate) fn cancel_request_watches_by_reply_id(
    db: &HcomDb,
    sender: &str,
    reply_to_id: i64,
    only: Option<&HashSet<String>>,
) {
    for (key, _sub, filters) in &load_reqwatch_subs(db, only) {
        let target = filters.get("target").and_then(|v| v.as_str()).unwrap_or("");
        let req_id = filters
            .get("request_id")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        if target == sender
            && req_id == reply_to_id
            && let Err(e) = db.kv_set(key, None)
        {
            crate::log::log_error(
                "db",
                "cancel_request_watches_by_reply.kv_set",
                &format!("{e}"),
            );
        }
    }
}

/// Send a system notification message.
pub(crate) fn send_system_message(
    db: &HcomDb,
    sender_name: &str,
    message: &str,
) -> Result<Vec<String>> {
    send_message_as(db, sender_name, "system", message)
}

/// Send a message from a specific sender kind.
pub(crate) fn send_message_as(
    db: &HcomDb,
    sender_name: &str,
    sender_kind: &str,
    message: &str,
) -> Result<Vec<String>> {
    send_reaction_message(db, sender_name, sender_kind, message, &[])
}

fn send_reaction_message(
    db: &HcomDb,
    sender_name: &str,
    sender_kind: &str,
    message: &str,
    reaction_chain: &[String],
) -> Result<Vec<String>> {
    let mut stmt = db.conn.prepare_cached("SELECT name, tag FROM instances")?;
    let instances: Vec<InstanceInfo> = stmt
        .query_map([], |row| {
            Ok(InstanceInfo {
                name: row.get::<_, String>(0)?,
                tag: row.get::<_, Option<String>>(1)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    let parsed_mentions = extract_mentions(message);
    let scope_result = if parsed_mentions.is_empty() {
        compute_scope(message, &instances, None).map_err(anyhow::Error::msg)?
    } else {
        let (mentions, _) =
            resolve_targets(&parsed_mentions, &instances).map_err(anyhow::Error::msg)?;
        ScopeResult {
            scope: MessageScope::Mentions,
            mentions,
        }
    };
    let (scope, mention_list, delivered_to) = if scope_result.scope == MessageScope::Broadcast {
        let delivered: Vec<String> = instances
            .iter()
            .filter(|inst| inst.name != sender_name)
            .map(|inst| inst.name.clone())
            .collect();
        ("broadcast".to_string(), vec![], delivered)
    } else {
        let delivered: Vec<String> = scope_result
            .mentions
            .iter()
            .filter(|n| n.as_str() != sender_name)
            .cloned()
            .collect();
        ("mentions".to_string(), scope_result.mentions, delivered)
    };

    let mut event_data = serde_json::json!({
        "from": sender_name,
        "sender_kind": sender_kind,
        "scope": scope,
        "text": message,
        "delivered_to": delivered_to,
    });
    if !mention_list.is_empty() {
        event_data["mentions"] = serde_json::json!(mention_list);
    }

    if !reaction_chain.is_empty() {
        event_data["reaction_subscriptions"] = json!(reaction_chain);
    }

    let routing_instance = match sender_kind {
        "instance" => sender_name.to_string(),
        "external" => format!("ext_{}", sender_name),
        _ => format!("sys_{}", sender_name),
    };
    db.log_event("message", &routing_instance, &event_data)?;

    Ok(delivered_to)
}

fn resolve_caller_kind(db: &HcomDb, caller: &str) -> &'static str {
    let exists: bool = db
        .conn()
        .query_row(
            "SELECT 1 FROM instances WHERE name = ?",
            rusqlite::params![caller],
            |_| Ok(true),
        )
        .unwrap_or(false);
    if exists { "instance" } else { "external" }
}

/// Rewrite collision SQL stored before the time-window index fix. Its
/// correlated scans walked every status event (~0.3s per subscription on each
/// file-write event), inside the writing agent's tool hook.
fn upgrade_collision_sql(sql: &str) -> Option<String> {
    if !sql.contains("WHERE e.type = 'status'") && !sql.contains("WHERE e2.type = 'status'") {
        return None;
    }
    let mut sql = sql
        .replace("WHERE e.type = 'status'", "WHERE +e.type = 'status'")
        .replace("WHERE e2.type = 'status'", "WHERE +e2.type = 'status'");
    for inner in ["e", "e2"] {
        sql = sql.replace(
            &format!(
                "AND ABS(strftime('%s', events_v.timestamp) - strftime('%s', {inner}.timestamp)) < 30"
            ),
            &format!("AND {}", collision_window_sql("events_v", inner)),
        );
    }
    Some(sql)
}

fn collision_self_relevance_sql(caller: &str) -> String {
    let caller_escaped = caller.replace('\'', "''");
    format!(
        "(events_v.instance = '{caller_escaped}' OR EXISTS (SELECT 1 FROM events_v e2 WHERE +e2.type = 'status' AND e2.status_context IN {ctx} AND e2.status_detail = events_v.status_detail AND e2.instance = '{caller_escaped}' AND {window}))",
        ctx = FILE_WRITE_CONTEXTS,
        window = collision_window_sql("events_v", "e2"),
    )
}

fn format_sub_notification(
    db: &HcomDb,
    sub_id: &str,
    event_id: i64,
    event_type: &str,
    instance: &str,
    data: &serde_json::Value,
    filters: Option<&serde_json::Value>,
) -> String {
    if let Some(f) = filters {
        if f.get("request_watch").is_some() {
            let request_id = f
                .get("request_id")
                .and_then(|v| v.as_i64())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "?".to_string());
            let target = f.get("target").and_then(|v| v.as_str()).unwrap_or(instance);
            let action = if event_type == "status" {
                "went idle"
            } else {
                "stopped"
            };
            return format!(
                "[sub:{}] #{} {} {} without responding to your request #{}",
                sub_id, event_id, target, action, request_id
            );
        }

        if f.get("collision").is_some() && event_type == "status" {
            let file_path = data.get("detail").and_then(|v| v.as_str()).unwrap_or("?");
            if let Some(partner) = find_collision_partner(db, event_id, instance, file_path) {
                return format!(
                    "\u{26a0}\u{fe0f} COLLISION [sub:{}] #{}: {} and {} both edited {}",
                    sub_id, event_id, instance, partner, file_path
                );
            }
            return format!(
                "\u{26a0}\u{fe0f} COLLISION [sub:{}] #{}: {} edited {} (conflict with another agent)",
                sub_id, event_id, instance, file_path
            );
        }
    }

    let mut parts = vec![
        format!("[sub:{}]", sub_id),
        format!("#{}", event_id),
        event_type.to_string(),
        instance.to_string(),
    ];

    match event_type {
        "message" => {
            let mut text = data
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if text.len() > 60 {
                let mut end = 57;
                while end > 0 && !text.is_char_boundary(end) {
                    end -= 1;
                }
                text = format!("{}...", &text[..end]);
            }
            text = text.replace('@', "(at)");
            let from = data.get("from").and_then(|v| v.as_str()).unwrap_or("?");
            parts.push(format!("from:{}", from));
            parts.push(format!("\"{}\"", text));
        }
        "status" => {
            parts.push(
                data.get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
                    .to_string(),
            );
            if let Some(ctx) = data.get("context").and_then(|v| v.as_str())
                && !ctx.is_empty()
            {
                parts.push(ctx.to_string());
                if let Some(detail) = data.get("detail").and_then(|v| v.as_str())
                    && !detail.is_empty()
                {
                    let truncated = if detail.len() > 40 {
                        if ctx.contains("Bash") {
                            let end = (0..=37)
                                .rev()
                                .find(|&i| detail.is_char_boundary(i))
                                .unwrap_or(0);
                            format!("{}...", &detail[..end])
                        } else {
                            let start = (detail.len().saturating_sub(37)..=detail.len())
                                .find(|&i| detail.is_char_boundary(i))
                                .unwrap_or(detail.len());
                            format!("...{}", &detail[start..])
                        }
                    } else {
                        detail.to_string()
                    };
                    parts.push(truncated);
                }
            }
        }
        "life" => {
            parts.push(
                data.get("action")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
                    .to_string(),
            );
            if let Some(by) = data.get("by").and_then(|v| v.as_str())
                && !by.is_empty()
            {
                parts.push(format!("by:{}", by));
            }
        }
        _ => {}
    }

    parts.join(" | ")
}

fn find_collision_partner(
    db: &HcomDb,
    event_id: i64,
    instance: &str,
    file_path: &str,
) -> Option<String> {
    db.conn
        .query_row(
            &format!(
                "SELECT e.instance FROM events_v ev, events_v e
                 WHERE ev.id = ?
                 AND +e.type = 'status' AND e.status_context IN {}
                 AND e.status_detail = ?
                 AND e.instance != ?
                 AND {}
                 ORDER BY e.id DESC LIMIT 1",
                FILE_WRITE_CONTEXTS,
                collision_window_sql("ev", "e"),
            ),
            params![event_id, file_path, instance],
            |row| row.get::<_, String>(0),
        )
        .ok()
}

fn send_sub_notification(db: &HcomDb, caller: &str, message: &str) -> bool {
    let row: Option<(String, Option<String>)> = db
        .conn
        .query_row(
            "SELECT name, tag FROM instances WHERE name = ?",
            params![caller],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .ok();

    let Some((name, tag)) = row else {
        return false;
    };

    let full_name = match tag.filter(|t| !t.is_empty()) {
        Some(t) => format!("{}-{}", t, name),
        None => name,
    };

    let text = format!("@{} {}", full_name, message);
    let Ok(delivered_to) = send_system_message(db, "[hcom-events]", &text) else {
        return false;
    };
    // send_system_message only logs the [hcom-events] row; unlike `hcom send`
    // it does not ping notify endpoints, so the notification can sit unread
    // until an unrelated wake. Wake only the matching caller here: this path
    // runs inline from log_event, so avoid a broader wake_all fan-out.
    if delivered_to.iter().any(|recipient| recipient == caller) {
        crate::notify::wake(db, caller, &[]);
    }
    true
}

/// SHA-256 hex hash.
fn sha256_hash(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    result.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::io::ErrorKind;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    fn setup_full_test_db() -> (HcomDb, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(10_000);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_subscriptions_{}_{}.db",
            std::process::id(),
            test_id
        ));

        let db = HcomDb::open_raw(&db_path).unwrap();
        db.init_db().unwrap();
        (db, db_path)
    }

    fn cleanup_test_db(path: PathBuf) {
        let _ = std::fs::remove_file(path);
    }

    fn bind_probe() -> TcpListener {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        listener
    }

    fn await_connect(listener: &TcpListener, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            match listener.accept() {
                Ok(_) => return true,
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return false,
            }
        }
    }

    fn count_reqwatch_without_reply_notifications(db: &HcomDb, requester: &str) -> i64 {
        let pattern = format!("%@{requester} %");
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE type = 'message'
                 AND json_extract(data, '$.text') LIKE '%without responding to your request%'
                 AND json_extract(data, '$.text') LIKE ?1",
                params![pattern],
                |row| row.get(0),
            )
            .unwrap_or(0)
    }

    fn setup_reqwatch_pair(
        db: &HcomDb,
        requester: &str,
        responder: &str,
        responder_tool: &str,
    ) -> i64 {
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, last_event_id, created_at)
                 VALUES (?1, 'claude', 0, 1000.0), (?2, ?3, 0, 1000.0)",
                params![requester, responder, responder_tool],
            )
            .unwrap();
        let req_data = serde_json::json!({
            "from": requester,
            "sender_kind": "instance",
            "scope": "mentions",
            "text": "ping",
            "delivered_to": [responder],
            "intent": "request",
            "mentions": [responder],
        });
        let request_id = db.log_event("message", requester, &req_data).unwrap();
        create_request_watches(db, requester, request_id, &[responder.to_string()]);
        db.conn()
            .execute(
                "UPDATE instances SET last_event_id = ?1 WHERE name = ?2",
                params![request_id, responder],
            )
            .unwrap();
        request_id
    }

    #[test]
    fn test_create_request_watches_records_antigravity_target_tool() {
        let (db, db_path) = setup_full_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, created_at) VALUES ('gora', 'claude', 1000.0), ('nabe', 'antigravity', 1000.0)",
                [],
            )
            .unwrap();
        create_request_watches(&db, "gora", 42, &[String::from("nabe")]);
        let sub_raw = db
            .kv_get("events_sub:reqwatch-42-nabe")
            .unwrap()
            .expect("reqwatch row");
        let sub: serde_json::Value = serde_json::from_str(&sub_raw).unwrap();
        assert_eq!(sub["filters"]["target_tool"].as_str(), Some("antigravity"));
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_agy_reqwatch_listening_defers_idle_notification() {
        let (db, db_path) = setup_full_test_db();
        let request_id = setup_reqwatch_pair(&db, "gora", "nabe", "antigravity");
        let before = count_reqwatch_without_reply_notifications(&db, "gora");

        let data = serde_json::json!({"status": "listening", "context": ""});
        db.log_event("status", "nabe", &data).unwrap();

        assert_eq!(
            count_reqwatch_without_reply_notifications(&db, "gora"),
            before,
            "agy listening should defer reqwatch notification"
        );
        let sub_raw = db
            .kv_get(&format!("events_sub:reqwatch-{request_id}-nabe"))
            .unwrap()
            .unwrap();
        let sub: serde_json::Value = serde_json::from_str(&sub_raw).unwrap();
        assert!(
            sub.get("idle_grace_until")
                .and_then(|v| v.as_f64())
                .is_some(),
            "grace should be armed: {sub}"
        );
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_agy_reqwatch_stopped_notifies_immediately() {
        let (db, db_path) = setup_full_test_db();
        let request_id = setup_reqwatch_pair(&db, "gora", "nabe", "antigravity");
        let before = count_reqwatch_without_reply_notifications(&db, "gora");

        let data = serde_json::json!({"action": "stopped", "by": "pty"});
        db.log_event("life", "nabe", &data).unwrap();

        assert_eq!(
            count_reqwatch_without_reply_notifications(&db, "gora"),
            before + 1,
            "agy stopped should notify without waiting for grace"
        );
        assert!(
            db.kv_get(&format!("events_sub:reqwatch-{request_id}-nabe"))
                .unwrap()
                .is_none(),
            "once sub should be removed after notify"
        );
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_gemini_reqwatch_listening_notifies_without_grace() {
        let (db, db_path) = setup_full_test_db();
        setup_reqwatch_pair(&db, "gora", "nova", "gemini");
        let before = count_reqwatch_without_reply_notifications(&db, "gora");

        let data = serde_json::json!({"status": "listening", "context": ""});
        db.log_event("status", "nova", &data).unwrap();

        assert_eq!(
            count_reqwatch_without_reply_notifications(&db, "gora"),
            before + 1,
            "non-agy listening should notify immediately"
        );
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_pi_reqwatch_notifies_after_delivery_active_then_listening() {
        let (db, db_path) = setup_full_test_db();
        setup_reqwatch_pair(&db, "gora", "nabe", "pi");
        let before = count_reqwatch_without_reply_notifications(&db, "gora");

        db.log_event(
            "status",
            "nabe",
            &serde_json::json!({"status": "active", "context": "deliver:gora"}),
        )
        .unwrap();
        assert_eq!(
            count_reqwatch_without_reply_notifications(&db, "gora"),
            before,
            "delivery active edge should not notify yet"
        );

        db.log_event(
            "status",
            "nabe",
            &serde_json::json!({"status": "listening", "context": ""}),
        )
        .unwrap();
        assert_eq!(
            count_reqwatch_without_reply_notifications(&db, "gora"),
            before + 1,
            "pi listening after delivery should notify"
        );
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_agy_reqwatch_active_clears_idle_grace() {
        let (db, db_path) = setup_full_test_db();
        let request_id = setup_reqwatch_pair(&db, "gora", "nabe", "antigravity");
        let sub_key = format!("events_sub:reqwatch-{request_id}-nabe");

        db.log_event(
            "status",
            "nabe",
            &serde_json::json!({"status": "listening", "context": ""}),
        )
        .unwrap();
        assert!(
            db.kv_get(&sub_key)
                .unwrap()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .and_then(|v| v.get("idle_grace_until").cloned())
                .is_some()
        );

        db.log_event(
            "status",
            "nabe",
            &serde_json::json!({"status": "active", "context": "tool:run_command"}),
        )
        .unwrap();

        let sub: serde_json::Value =
            serde_json::from_str(&db.kv_get(&sub_key).unwrap().unwrap()).unwrap();
        assert!(
            sub.get("idle_grace_until").is_none(),
            "active should clear agy grace: {sub}"
        );
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_agy_reqwatch_expired_grace_sweep_notifies_exactly_once() {
        let (db, db_path) = setup_full_test_db();
        let request_id = setup_reqwatch_pair(&db, "gora", "nabe", "antigravity");
        let sub_key = format!("events_sub:reqwatch-{request_id}-nabe");
        let before = count_reqwatch_without_reply_notifications(&db, "gora");

        db.set_status("nabe", "listening", "").unwrap();
        db.log_event(
            "status",
            "nabe",
            &serde_json::json!({"status": "listening", "context": ""}),
        )
        .unwrap();

        let mut sub: serde_json::Value =
            serde_json::from_str(&db.kv_get(&sub_key).unwrap().unwrap()).unwrap();
        sub["idle_grace_until"] = serde_json::json!(1.0);
        kv_store_sub(&db, &sub_key, &sub);

        sweep_expired_reqwatch_graces(&db, 2.0);
        sweep_expired_reqwatch_graces(&db, 3.0);

        assert_eq!(
            count_reqwatch_without_reply_notifications(&db, "gora"),
            before + 1,
            "expired grace should emit one abandoned-request notice"
        );
        assert!(
            db.kv_get(&sub_key).unwrap().is_none(),
            "one-shot reqwatch should be claimed and removed"
        );
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_agy_reqwatch_reply_during_grace_cancels_expiry() {
        let (db, db_path) = setup_full_test_db();
        let request_id = setup_reqwatch_pair(&db, "gora", "nabe", "antigravity");
        let sub_key = format!("events_sub:reqwatch-{request_id}-nabe");
        let before = count_reqwatch_without_reply_notifications(&db, "gora");

        db.set_status("nabe", "listening", "").unwrap();
        db.log_event(
            "status",
            "nabe",
            &serde_json::json!({"status": "listening", "context": ""}),
        )
        .unwrap();
        db.log_event(
            "message",
            "nabe",
            &serde_json::json!({
                "from": "nabe",
                "sender_kind": "instance",
                "scope": "mentions",
                "text": "done",
                "mentions": ["gora"],
                "delivered_to": ["gora"],
                "reply_to_local": request_id,
            }),
        )
        .unwrap();

        sweep_expired_reqwatch_graces(&db, f64::MAX);
        assert!(db.kv_get(&sub_key).unwrap().is_none());
        assert_eq!(
            count_reqwatch_without_reply_notifications(&db, "gora"),
            before,
            "a reply during grace must suppress the idle notice"
        );
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_agy_reqwatch_later_idle_spell_arms_fresh_grace() {
        let (db, db_path) = setup_full_test_db();
        let request_id = setup_reqwatch_pair(&db, "gora", "nabe", "antigravity");
        let sub_key = format!("events_sub:reqwatch-{request_id}-nabe");

        db.set_status("nabe", "listening", "").unwrap();
        let first_idle = db
            .log_event(
                "status",
                "nabe",
                &serde_json::json!({"status": "listening", "context": ""}),
            )
            .unwrap();

        db.set_status("nabe", "blocked", "approval").unwrap();
        db.log_event(
            "status",
            "nabe",
            &serde_json::json!({"status": "blocked", "context": "approval"}),
        )
        .unwrap();
        let after_blocked: serde_json::Value =
            serde_json::from_str(&db.kv_get(&sub_key).unwrap().unwrap()).unwrap();
        assert!(after_blocked.get("idle_grace_until").is_none());
        assert!(after_blocked.get("idle_grace_event_id").is_none());

        db.set_status("nabe", "listening", "").unwrap();
        let second_idle = db
            .log_event(
                "status",
                "nabe",
                &serde_json::json!({"status": "listening", "context": ""}),
            )
            .unwrap();
        let rearmed: serde_json::Value =
            serde_json::from_str(&db.kv_get(&sub_key).unwrap().unwrap()).unwrap();
        assert_ne!(first_idle, second_idle);
        assert_eq!(
            rearmed.get("idle_grace_event_id").and_then(|v| v.as_i64()),
            Some(second_idle),
            "the later idle spell must own a fresh grace timer"
        );
        assert!(rearmed.get("idle_grace_until").is_some());
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_sha256_hash() {
        let h1 = sha256_hash("test input");
        let h2 = sha256_hash("test input");
        let h3 = sha256_hash("different input");
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
        assert_eq!(h1.len(), 64);
        assert_eq!(&h1[..8], "9dfe6f15");
    }

    #[test]
    fn test_upgrade_collision_sql_matches_current_builder() {
        let mut filters = crate::core::filters::FilterMap::new();
        filters.insert("collision".into(), vec!["true".into()]);
        let current = format!(
            "({}) AND {}",
            build_sql_from_flags(&filters).unwrap(),
            collision_self_relevance_sql("luna")
        );
        let legacy = current
            .replace("+e.type", "e.type")
            .replace("+e2.type", "e2.type")
            .replace(
                &collision_window_sql("events_v", "e"),
                "ABS(strftime('%s', events_v.timestamp) - strftime('%s', e.timestamp)) < 30",
            )
            .replace(
                &collision_window_sql("events_v", "e2"),
                "ABS(strftime('%s', events_v.timestamp) - strftime('%s', e2.timestamp)) < 30",
            );
        assert_ne!(legacy, current);
        assert_eq!(
            upgrade_collision_sql(&legacy).as_deref(),
            Some(current.as_str())
        );
        assert_eq!(upgrade_collision_sql(&current), None);
    }

    #[test]
    fn test_process_logged_event_upgrades_stored_collision_sql() {
        let (db, path) = setup_full_test_db();
        let legacy = "(type = 'status' AND EXISTS (SELECT 1 FROM events_v e WHERE e.type = 'status' \
             AND ABS(strftime('%s', events_v.timestamp) - strftime('%s', e.timestamp)) < 30))";
        let sub = json!({"id": "sub-legacy", "caller": "luna", "sql": legacy, "last_id": 0,
            "filters": {"collision": ["true"]}});
        db.kv_set("events_sub:sub-legacy", Some(&sub.to_string()))
            .unwrap();
        db.log_status_event("nova", "active", "tool:Bash", Some("ls"), None)
            .unwrap();
        let stored: serde_json::Value =
            serde_json::from_str(&db.kv_get("events_sub:sub-legacy").unwrap().unwrap()).unwrap();
        let sql = stored["sql"].as_str().unwrap();
        assert!(sql.contains("WHERE +e.type = 'status'"));
        assert!(sql.contains(&collision_window_sql("events_v", "e")));
        assert_eq!(stored["caller"], "luna");

        let raw = json!({"id": "sub-raw", "caller": "luna", "sql": legacy, "last_id": 0,
            "filters": {}});
        db.kv_set("events_sub:sub-raw", Some(&raw.to_string()))
            .unwrap();
        db.log_status_event("nova", "active", "tool:Bash", Some("pwd"), None)
            .unwrap();
        let stored: serde_json::Value =
            serde_json::from_str(&db.kv_get("events_sub:sub-raw").unwrap().unwrap()).unwrap();
        assert_eq!(stored["sql"], legacy);
        cleanup_test_db(path);
    }

    #[test]
    fn test_collision_subscription_fires_on_edit_and_skips_other_events() {
        let (db, path) = setup_full_test_db();
        for name in ["luna", "nova"] {
            db.conn
                .execute(
                    "INSERT INTO instances (name, created_at) VALUES (?, 1000.0)",
                    params![name],
                )
                .unwrap();
        }
        let mut filters = crate::core::filters::FilterMap::new();
        filters.insert("collision".into(), vec!["true".into()]);
        let sql = format!(
            "({}) AND {}",
            build_sql_from_flags(&filters).unwrap(),
            collision_self_relevance_sql("luna")
        );
        let sub = json!({"id": "sub-col", "caller": "luna", "sql": sql, "last_id": 0,
            "filters": {"collision": ["true"]}});
        db.kv_set("events_sub:sub-col", Some(&sub.to_string()))
            .unwrap();
        let notices = || -> i64 {
            db.conn
                .query_row(
                    "SELECT COUNT(*) FROM events_v WHERE type = 'message' AND msg_from = '[hcom-events]'",
                    [],
                    |r| r.get(0),
                )
                .unwrap()
        };

        db.log_status_event("luna", "active", "tool:Bash", Some("src/a.rs"), None)
            .unwrap();
        db.log_status_event("nova", "active", "tool:Bash", Some("src/a.rs"), None)
            .unwrap();
        assert_eq!(notices(), 0);

        db.log_status_event("luna", "active", "tool:Edit", Some("src/a.rs"), None)
            .unwrap();
        db.log_status_event("nova", "active", "tool:Edit", Some("src/a.rs"), None)
            .unwrap();
        assert_eq!(notices(), 1);
        cleanup_test_db(path);
    }

    #[test]
    fn test_find_collision_partner_respects_window() {
        let (db, path) = setup_full_test_db();
        let edit = |instance: &str, ts: &str| {
            db.log_status_event(instance, "active", "tool:Edit", Some("/r/a.rs"), Some(ts))
                .unwrap();
            db.get_last_event_id()
        };
        edit("luna", "2026-09-30T02:00:00.100000+00:00");
        edit("nova", "2026-09-30T02:01:00.000000+00:00");
        let near = edit("mira", "2026-09-30T02:01:29.900000+00:00");
        assert_eq!(
            find_collision_partner(&db, near, "mira", "/r/a.rs").as_deref(),
            Some("nova")
        );
        let far = edit("mira", "2026-09-30T02:05:00Z");
        assert_eq!(find_collision_partner(&db, far, "mira", "/r/a.rs"), None);
        cleanup_test_db(path);
    }

    #[test]
    fn test_collision_self_relevance_matches_filter_constants() {
        let sql = collision_self_relevance_sql("luna");
        assert!(sql.contains(FILE_WRITE_CONTEXTS));
        assert!(sql.contains("< 30"));
        assert!(!sql.contains("tool:edit_file"));
        assert!(!sql.contains("< 20"));
    }

    #[test]
    fn test_subscription_recursion_guard_sys_prefix() {
        let (db, db_path) = setup_full_test_db();

        // Create a subscription
        let sub = serde_json::json!({
            "id": "test-sub",
            "caller": "luna",
            "sql": "type = 'message'",
            "last_id": 0
        });
        db.kv_set("events_sub:test", Some(&sub.to_string()))
            .unwrap();

        // Log event from sys_ instance - should NOT trigger subscription
        let data = serde_json::json!({"from": "[hcom-events]", "text": "test"});
        db.log_event("message", "sys_[hcom-events]", &data).unwrap();

        // Sub should not be updated (last_id should still be 0)
        let sub_after = db.kv_get("events_sub:test").unwrap().unwrap();
        let sub_val: serde_json::Value = serde_json::from_str(&sub_after).unwrap();
        assert_eq!(sub_val["last_id"], 0);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_subscription_recursion_guard_system_sender_kind() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0)",
                [],
            )
            .unwrap();

        let sub = serde_json::json!({
            "id": "test-sub",
            "caller": "luna",
            "sql": "type = 'message'",
            "last_id": 0
        });
        db.kv_set("events_sub:test", Some(&sub.to_string()))
            .unwrap();

        // Log system message - recursion guard should skip
        let data = serde_json::json!({
            "from": "[hcom-events]",
            "sender_kind": "system",
            "text": "notification"
        });
        db.log_event("message", "ext_test", &data).unwrap();

        // Sub should not be updated
        let sub_after = db.kv_get("events_sub:test").unwrap().unwrap();
        let sub_val: serde_json::Value = serde_json::from_str(&sub_after).unwrap();
        assert_eq!(sub_val["last_id"], 0);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_subscription_matches_and_updates_cursor() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('luna', '', 1000.0)",
                [],
            )
            .unwrap();

        // Create subscription that matches all status events
        let sub = serde_json::json!({
            "id": "test-sub",
            "caller": "luna",
            "sql": "type = 'status'",
            "last_id": 0
        });
        db.kv_set("events_sub:test", Some(&sub.to_string()))
            .unwrap();

        // Log a status event (not from sys_, not system sender_kind)
        let data = serde_json::json!({"status": "active", "context": "test"});
        let event_id = db.log_event("status", "nova", &data).unwrap();

        // Sub should be updated with new last_id
        let sub_after = db.kv_get("events_sub:test").unwrap().unwrap();
        let sub_val: serde_json::Value = serde_json::from_str(&sub_after).unwrap();
        assert_eq!(sub_val["last_id"], event_id);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_subscription_once_removes_after_match() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('luna', '', 1000.0)",
                [],
            )
            .unwrap();

        let sub = serde_json::json!({
            "id": "once-sub",
            "caller": "luna",
            "sql": "type = 'status'",
            "once": true,
            "last_id": 0
        });
        db.kv_set("events_sub:once-test", Some(&sub.to_string()))
            .unwrap();

        // Log a matching event
        let data = serde_json::json!({"status": "active"});
        db.log_event("status", "nova", &data).unwrap();

        // Subscription should be removed
        assert!(db.kv_get("events_sub:once-test").unwrap().is_none());

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_on_hit_does_not_consume_its_own_output() {
        for once in [false, true] {
            for kind in ["instance", "external"] {
                let (db, path) = setup_full_test_db();
                db.conn
                    .execute(
                        "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0)",
                        [],
                    )
                    .unwrap();
                let sub = json!({
                    "id": "reaction", "caller": "luna", "caller_kind": kind,
                    "sql": "type = 'message'", "last_id": 0, "once": once,
                    "on_hit_text": "reaction output",
                });
                db.kv_set("events_sub:reaction", Some(&sub.to_string()))
                    .unwrap();
                let data = json!({"from": "nova", "sender_kind": "instance", "text": "trigger"});
                for expected in 1..=2 {
                    let id = db.log_event("message", "nova", &data).unwrap();
                    let count: i64 = db.conn.query_row(
                        "SELECT COUNT(*) FROM events WHERE json_extract(data, '$.text') = 'reaction output'",
                        [], |r| r.get(0),
                    ).unwrap();
                    assert_eq!(count, if once { 1 } else { expected });
                    if once {
                        assert!(db.kv_get("events_sub:reaction").unwrap().is_none());
                    } else {
                        let stored: serde_json::Value = serde_json::from_str(
                            &db.kv_get("events_sub:reaction").unwrap().unwrap(),
                        )
                        .unwrap();
                        assert_eq!(stored["last_id"], id);
                        // Replay from a different processor must also skip our output.
                        let (output_id, output): (i64, String) = db.conn.query_row(
                            "SELECT id, data FROM events WHERE json_extract(data, '$.text') = 'reaction output' ORDER BY id DESC LIMIT 1",
                            [], |r| Ok((r.get(0)?, r.get(1)?)),
                        ).unwrap();
                        process_logged_event(
                            &db,
                            output_id,
                            "message",
                            "luna",
                            &serde_json::from_str(&output).unwrap(),
                        );
                        assert_eq!(db.get_last_event_id(), output_id);
                    }
                }
                cleanup_test_db(path);
            }
        }
    }

    #[test]
    fn test_on_hit_reaction_chain_stops_indirect_recursion() {
        let (db, path) = setup_full_test_db();
        for (id, sender) in [("first", "nova"), ("second", "luna")] {
            let sub = json!({
                "id": id, "caller": if id == "first" { "luna" } else { "nova" },
                "caller_kind": "external", "sql": format!("type = 'message' AND msg_from = '{sender}'"),
                "last_id": 0, "on_hit_text": id,
            });
            db.kv_set(&format!("events_sub:{id}"), Some(&sub.to_string()))
                .unwrap();
        }
        db.log_event(
            "message",
            "nova",
            &json!({"from": "nova", "text": "trigger"}),
        )
        .unwrap();
        assert_eq!(db.get_last_event_id(), 3);
        let chain: String = db
            .conn
            .query_row(
                "SELECT json_extract(data, '$.reaction_subscriptions') FROM events WHERE id = 3",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&chain).unwrap(),
            json!(["first", "second"])
        );
        cleanup_test_db(path);
    }

    #[test]
    fn test_subscription_sql_error_graceful() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('luna', '', 1000.0)",
                [],
            )
            .unwrap();

        // Bad SQL subscription
        let bad_sub = serde_json::json!({
            "id": "bad-sql",
            "caller": "luna",
            "sql": "INVALID SQL %%% BROKEN",
            "last_id": 0
        });
        db.kv_set("events_sub:bad", Some(&bad_sub.to_string()))
            .unwrap();

        // Good SQL subscription
        let good_sub = serde_json::json!({
            "id": "good-sql",
            "caller": "luna",
            "sql": "type = 'status'",
            "last_id": 0
        });
        db.kv_set("events_sub:good", Some(&good_sub.to_string()))
            .unwrap();

        // Log a matching event — should not crash despite bad SQL sub
        let data = serde_json::json!({"status": "active"});
        let event_id = db.log_event("status", "nova", &data).unwrap();

        // Bad sub should remain untouched (last_id still 0)
        let bad_after = db.kv_get("events_sub:bad").unwrap().unwrap();
        let bad_val: serde_json::Value = serde_json::from_str(&bad_after).unwrap();
        assert_eq!(bad_val["last_id"], 0, "Bad SQL sub should not advance");

        // Good sub should have fired
        let good_after = db.kv_get("events_sub:good").unwrap().unwrap();
        let good_val: serde_json::Value = serde_json::from_str(&good_after).unwrap();
        assert_eq!(good_val["last_id"], event_id, "Good sub should advance");

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_cancel_request_watches_by_flow() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('requester', '', 1000.0)",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('responder', '', 1000.0)",
                [],
            )
            .unwrap();

        // Create a request-watch subscription
        let reqwatch = serde_json::json!({
            "id": "reqwatch-test",
            "caller": "requester",
            "sql": "type = 'status'",
            "last_id": 0,
            "once": true,
            "filters": {
                "request_watch": true,
                "target": "responder",
                "request_id": 42
            }
        });
        db.kv_set("events_sub:reqwatch-test", Some(&reqwatch.to_string()))
            .unwrap();

        // Simulate responder replying to requester with reply_to matching request_id
        cancel_request_watches_by_flow(
            &db,
            "responder",
            &["requester".to_string()],
            Some(42),
            None,
        );

        // Subscription should be deleted
        assert!(
            db.kv_get("events_sub:reqwatch-test").unwrap().is_none(),
            "Request-watch should be cancelled when target replies"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_cancel_request_watches_wrong_reply_id() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('requester', '', 1000.0)",
                [],
            )
            .unwrap();

        let reqwatch = serde_json::json!({
            "id": "reqwatch-test2",
            "caller": "requester",
            "sql": "type = 'status'",
            "last_id": 0,
            "once": true,
            "filters": {
                "request_watch": true,
                "target": "responder",
                "request_id": 42
            }
        });
        db.kv_set("events_sub:reqwatch-test2", Some(&reqwatch.to_string()))
            .unwrap();

        // Reply with wrong request_id — should NOT cancel
        cancel_request_watches_by_flow(
            &db,
            "responder",
            &["requester".to_string()],
            Some(99),
            None,
        );

        assert!(
            db.kv_get("events_sub:reqwatch-test2").unwrap().is_some(),
            "Request-watch should NOT be cancelled for mismatched reply_to"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_cancel_request_watches_by_reply_id_via_log_event() {
        // End-to-end: log a broadcast message with reply_to_local → should cancel reqwatch via Path 2
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('requester', '', 1000.0)",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('responder', '', 1000.0)",
                [],
            )
            .unwrap();

        // First, log a request message so we have an event_id to reply to
        let req_data = serde_json::json!({
            "from": "requester",
            "sender_kind": "instance",
            "scope": "mentions",
            "text": "do the thing",
            "delivered_to": ["responder"],
            "intent": "request",
            "mentions": ["responder"]
        });
        let request_id = db.log_event("message", "requester", &req_data).unwrap();

        // Create a request-watch subscription
        let reqwatch = serde_json::json!({
            "id": format!("reqwatch-{}-responder", request_id),
            "caller": "requester",
            "sql": "(type='status' AND instance=? AND status_val='listening')",
            "params": ["responder"],
            "last_id": request_id,
            "once": true,
            "filters": {
                "request_watch": true,
                "target": "responder",
                "request_id": request_id
            }
        });
        let sub_key = format!("events_sub:reqwatch-{}-responder", request_id);
        db.kv_set(&sub_key, Some(&reqwatch.to_string())).unwrap();

        // Now log a BROADCAST ack from responder with reply_to_local = request_id
        let ack_data = serde_json::json!({
            "from": "responder",
            "sender_kind": "instance",
            "scope": "broadcast",
            "text": "done with the task",
            "delivered_to": ["requester"],
            "intent": "ack",
            "reply_to": request_id.to_string(),
            "reply_to_local": request_id
        });
        db.log_event("message", "responder", &ack_data).unwrap();

        // Reqwatch should be cancelled via Path 2
        assert!(
            db.kv_get(&sub_key).unwrap().is_none(),
            "Request-watch should be cancelled when target sends broadcast with reply_to_local matching request_id"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_subscription_recursion_guard_hcom_events_sender() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('luna', '', 1000.0)",
                [],
            )
            .unwrap();

        let sub = serde_json::json!({
            "id": "test-sub",
            "caller": "luna",
            "sql": "type = 'message'",
            "last_id": 0
        });
        db.kv_set("events_sub:test", Some(&sub.to_string()))
            .unwrap();

        // Log message from [hcom-events] (non-sys_ instance) — guard B should skip
        let data = serde_json::json!({
            "from": "[hcom-events]",
            "text": "notification from events"
        });
        db.log_event("message", "ext_notifier", &data).unwrap();

        // Sub should not be updated
        let sub_after = db.kv_get("events_sub:test").unwrap().unwrap();
        let sub_val: serde_json::Value = serde_json::from_str(&sub_after).unwrap();
        assert_eq!(
            sub_val["last_id"], 0,
            "[hcom-events] sender should be blocked by guard B"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_send_system_message_broadcast() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0)",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('nova', 1000.0)",
                [],
            )
            .unwrap();

        // No @mentions = broadcast
        let delivered = db
            .send_system_message("[hcom-test]", "hello everyone")
            .unwrap();
        assert_eq!(delivered.len(), 2);
        assert!(delivered.contains(&"luna".to_string()));
        assert!(delivered.contains(&"nova".to_string()));

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_send_system_message_targeted() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('luna', '', 1000.0)",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('nova', '', 1000.0)",
                [],
            )
            .unwrap();

        // With @mention = targeted
        let delivered = db
            .send_system_message("[hcom-test]", "@luna your task is done")
            .unwrap();
        assert_eq!(delivered.len(), 1);
        assert!(delivered.contains(&"luna".to_string()));

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_send_system_message_with_tag() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('luna', 'api', 1000.0)",
                [],
            )
            .unwrap();

        // Mention by full name (tag-name)
        let delivered = db
            .send_system_message("[hcom-test]", "@api-luna your task is done")
            .unwrap();
        assert_eq!(delivered.len(), 1);
        assert!(delivered.contains(&"luna".to_string()));

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_send_sub_notification_wakes_target_instance() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('tofu', '', 1000.0), ('rune', '', 1000.0)",
                [],
            )
            .unwrap();
        let tofu_probe = bind_probe();
        let rune_probe = bind_probe();
        db.upsert_notify_endpoint("tofu", "plugin", tofu_probe.local_addr().unwrap().port())
            .unwrap();
        db.upsert_notify_endpoint("rune", "plugin", rune_probe.local_addr().unwrap().port())
            .unwrap();

        assert!(send_sub_notification(
            &db,
            "tofu",
            "[sub:test] #42 dani status | blocked | approval"
        ));

        assert!(
            await_connect(&tofu_probe, Duration::from_millis(500)),
            "subscription notification should wake the subscribed instance"
        );
        assert!(
            !await_connect(&rune_probe, Duration::from_millis(100)),
            "subscription notification should not broadcast-wake unrelated instances"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_send_system_message_exact_name_avoids_tag_prefix_collision() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES
                 ('giru', '', 1000.0),
                 ('lasa', 'giru-test', 1000.0)",
                [],
            )
            .unwrap();

        let delivered = db
            .send_system_message("[hcom-test]", "@giru request timed out")
            .unwrap();
        assert_eq!(delivered, vec!["giru"]);

        let (mentions, delivered_to): (String, String) = db
            .conn
            .query_row(
                "SELECT json_extract(data, '$.mentions'),
                        json_extract(data, '$.delivered_to')
                 FROM events
                 WHERE type = 'message'
                 ORDER BY id DESC
                 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(mentions, r#"["giru"]"#);
        assert_eq!(delivered_to, r#"["giru"]"#);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_reqwatch_reply_requires_exact_delivered_to_member() {
        let (db, db_path) = setup_full_test_db();

        let near_match = serde_json::json!({
            "from": "lasa",
            "scope": "mentions",
            "mentions": ["giru2"],
            "delivered_to": ["giru2"],
            "text": "not for giru",
        });
        let near_id = db.log_event("message", "lasa", &near_match).unwrap();
        assert!(!reqwatch_reply_exists(&db, 0, "lasa", "giru"));

        let exact_match = serde_json::json!({
            "from": "lasa",
            "scope": "mentions",
            "mentions": ["giru"],
            "delivered_to": ["giru"],
            "text": "for giru",
        });
        db.log_event("message", "lasa", &exact_match).unwrap();
        assert!(reqwatch_reply_exists(&db, near_id, "lasa", "giru"));

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_on_hit_provenance_instance_caller() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0), ('nova', 1000.0)",
                [],
            )
            .unwrap();

        let sub = serde_json::json!({
            "id": "sub-onhit1",
            "caller": "luna",
            "caller_kind": "instance",
            "sql": "type = 'message' AND msg_from = 'nova'",
            "created": 1000.0,
            "last_id": 0,
            "once": false,
            "on_hit_text": "starting review now",
        });
        db.kv_set("events_sub:sub-onhit1", Some(&sub.to_string()))
            .unwrap();

        db.log_event(
            "message",
            "nova",
            &serde_json::json!({
                "from": "nova",
                "sender_kind": "instance",
                "scope": "broadcast",
                "text": "heads up",
                "delivered_to": ["luna"],
            }),
        )
        .unwrap();

        // Find the on-hit event: from=luna, sender_kind=instance, text matches
        let row: Option<(String, String)> = db
            .conn
            .query_row(
                "SELECT json_extract(data, '$.sender_kind'), json_extract(data, '$.text') \
                 FROM events WHERE json_extract(data, '$.from') = 'luna' \
                 AND json_extract(data, '$.text') = 'starting review now' LIMIT 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .ok();
        assert!(row.is_some(), "on-hit message should be logged");
        let (kind, text) = row.unwrap();
        assert_eq!(
            kind, "instance",
            "caller 'luna' is an instance → sender_kind=instance"
        );
        assert_eq!(
            text, "starting review now",
            "on-hit text sent verbatim, no @-prefix"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_on_hit_external_caller_and_mention_routing() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('dbadmin', 1000.0), ('nova', 1000.0)",
                [],
            )
            .unwrap();

        // Caller 'bigboss' is NOT in instances → external kind.
        // on_hit_text mentions @dbadmin → normal mention routing must deliver to dbadmin only.
        let sub = serde_json::json!({
            "id": "sub-onhit2",
            "caller": "bigboss",
            "caller_kind": "external",
            "sql": "type = 'message' AND msg_from = 'nova'",
            "created": 1000.0,
            "last_id": 0,
            "once": false,
            "on_hit_text": "@dbadmin review the change",
        });
        db.kv_set("events_sub:sub-onhit2", Some(&sub.to_string()))
            .unwrap();

        db.log_event(
            "message",
            "nova",
            &serde_json::json!({
                "from": "nova",
                "sender_kind": "instance",
                "scope": "broadcast",
                "text": "trigger",
                "delivered_to": ["dbadmin"],
            }),
        )
        .unwrap();

        let row: Option<(String, String, String)> = db
            .conn
            .query_row(
                "SELECT json_extract(data, '$.sender_kind'), \
                        json_extract(data, '$.scope'), \
                        json_extract(data, '$.delivered_to') \
                 FROM events WHERE json_extract(data, '$.from') = 'bigboss' LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .ok();
        assert!(
            row.is_some(),
            "on-hit message from bigboss should be logged"
        );
        let (kind, scope, delivered) = row.unwrap();
        assert_eq!(
            kind, "external",
            "non-instance caller → sender_kind=external"
        );
        assert_eq!(scope, "mentions", "text contains @mention → mentions scope");
        assert!(
            delivered.contains("dbadmin"),
            "delivered_to must include dbadmin"
        );
        assert!(
            !delivered.contains("bigboss"),
            "caller itself is not auto-mentioned"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_on_hit_caller_kind_captured_at_creation() {
        // Verify resolve_caller_kind via create_filter_subscription:
        // instance caller → caller_kind=instance
        // non-instance caller (e.g. bigboss from -b) → caller_kind=external
        use std::collections::HashMap;

        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0)",
                [],
            )
            .unwrap();

        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("agent".to_string(), vec!["luna".to_string()]);
        filters.insert("status".to_string(), vec!["listening".to_string()]);

        create_filter_subscription(&db, &filters, &[], "luna", false, Some("hi")).unwrap();
        create_filter_subscription(&db, &filters, &[], "bigboss", false, Some("hi")).unwrap();

        let luna_kind: String = db
            .conn
            .query_row(
                "SELECT json_extract(value, '$.caller_kind') FROM kv \
                 WHERE key LIKE 'events_sub:%' \
                 AND json_extract(value, '$.caller') = 'luna' LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(luna_kind, "instance");

        let bb_kind: String = db
            .conn
            .query_row(
                "SELECT json_extract(value, '$.caller_kind') FROM kv \
                 WHERE key LIKE 'events_sub:%' \
                 AND json_extract(value, '$.caller') = 'bigboss' LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(bb_kind, "external");

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_on_hit_provenance_stable_after_caller_stops() {
        // Sub created by an instance stays sender_kind=instance at fire time
        // even if that instance row has been deleted before the match.
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0), ('nova', 1000.0)",
                [],
            )
            .unwrap();

        let sub = serde_json::json!({
            "id": "sub-stab1",
            "caller": "luna",
            "caller_kind": "instance",
            "sql": "type = 'message' AND msg_from = 'nova'",
            "created": 1000.0,
            "last_id": 0,
            "once": false,
            "on_hit_text": "still luna",
        });
        db.kv_set("events_sub:sub-stab1", Some(&sub.to_string()))
            .unwrap();

        // Caller disappears before the sub fires.
        db.conn
            .execute("DELETE FROM instances WHERE name = 'luna'", [])
            .unwrap();

        db.log_event(
            "message",
            "nova",
            &serde_json::json!({
                "from": "nova",
                "sender_kind": "instance",
                "scope": "broadcast",
                "text": "trigger",
                "delivered_to": [],
            }),
        )
        .unwrap();

        let kind: Option<String> = db
            .conn
            .query_row(
                "SELECT json_extract(data, '$.sender_kind') FROM events \
                 WHERE json_extract(data, '$.from') = 'luna' \
                 AND json_extract(data, '$.text') = 'still luna' LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok();
        assert_eq!(
            kind.as_deref(),
            Some("instance"),
            "provenance captured at creation must survive caller stop"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_on_hit_unmatched_mention_delivers_to_nobody() {
        // Documents current behavior: an on-hit text mentioning a nonexistent
        // agent produces a mentions-scope event with empty delivered_to.
        // This mirrors how send_system_message behaves for typos — no error,
        // no fallback to broadcast. If we ever tighten mention validation for
        // on-hit, update this test.
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0), ('nova', 1000.0)",
                [],
            )
            .unwrap();

        let sub = serde_json::json!({
            "id": "sub-typo1",
            "caller": "luna",
            "caller_kind": "instance",
            "sql": "type = 'message' AND msg_from = 'nova'",
            "created": 1000.0,
            "last_id": 0,
            "once": false,
            "on_hit_text": "@notarealagent hello",
        });
        db.kv_set("events_sub:sub-typo1", Some(&sub.to_string()))
            .unwrap();

        db.log_event(
            "message",
            "nova",
            &serde_json::json!({
                "from": "nova",
                "sender_kind": "instance",
                "scope": "broadcast",
                "text": "trigger",
                "delivered_to": [],
            }),
        )
        .unwrap();

        let row: Option<(String, String)> = db
            .conn
            .query_row(
                "SELECT json_extract(data, '$.scope'), \
                        json_extract(data, '$.delivered_to') \
                 FROM events WHERE json_extract(data, '$.from') = 'luna' \
                 AND json_extract(data, '$.text') = '@notarealagent hello' LIMIT 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .ok();
        assert!(row.is_some(), "on-hit message should still be logged");
        let (scope, delivered) = row.unwrap();
        assert_eq!(
            scope, "mentions",
            "unmatched @ still produces mentions scope"
        );
        assert_eq!(delivered, "[]", "nobody matched → empty delivered_to");

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_delivery_only_subscription_does_not_emit_notifications() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0), ('nova', 1000.0)",
                [],
            )
            .unwrap();

        let member = serde_json::json!({
            "id": "sub-thread123",
            "caller": "luna",
            "thread_name": "debate-1",
            "auto_thread_member": true,
            "delivery_only": true,
            "created": 1000.0,
            "last_id": 0,
            "once": false
        });
        db.kv_set("events_sub:sub-thread123", Some(&member.to_string()))
            .unwrap();

        let data = serde_json::json!({
            "from": "nova",
            "sender_kind": "instance",
            "scope": "broadcast",
            "text": "hello",
            "delivered_to": ["luna"]
        });
        db.log_event("message", "nova", &data).unwrap();

        let count: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            count, 1,
            "delivery-only subscriptions must not create notifications"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_delivery_only_subscription_does_not_emit_status_notifications() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0), ('nova', 1000.0)",
                [],
            )
            .unwrap();

        let member = serde_json::json!({
            "id": "sub-thread123",
            "caller": "luna",
            "thread_name": "debate-1",
            "auto_thread_member": true,
            "delivery_only": true,
            "created": 1000.0,
            "last_id": 0,
            "once": false
        });
        db.kv_set("events_sub:sub-thread123", Some(&member.to_string()))
            .unwrap();

        let data = serde_json::json!({
            "status": "active",
            "context": "tool:shell",
            "detail": "hcom listen 1 --name nova"
        });
        db.log_event("status", "nova", &data).unwrap();

        let count: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            count, 1,
            "delivery-only subscriptions must not create status notifications"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_cleanup_subscriptions_keeps_delivery_only_memberships() {
        let (db, db_path) = setup_full_test_db();

        let normal = serde_json::json!({
            "id": "sub-normal",
            "caller": "luna",
            "sql": "type = 'message'",
            "last_id": 0
        });
        let thread_member = serde_json::json!({
            "id": "sub-thread",
            "caller": "luna",
            "thread_name": "debate-1",
            "auto_thread_member": true,
            "delivery_only": true,
            "created": 1000.0,
            "last_id": 0
        });
        db.kv_set("events_sub:sub-normal", Some(&normal.to_string()))
            .unwrap();
        db.kv_set("events_sub:sub-thread", Some(&thread_member.to_string()))
            .unwrap();

        let deleted = db.cleanup_subscriptions("luna").unwrap();
        assert_eq!(deleted, 1);
        assert!(db.kv_get("events_sub:sub-normal").unwrap().is_none());
        assert!(db.kv_get("events_sub:sub-thread").unwrap().is_some());

        cleanup_test_db(db_path);
    }

    fn notices_to(db: &HcomDb, name: &str) -> i64 {
        db.conn
            .query_row(
                "SELECT COUNT(*) FROM events_v WHERE type = 'message'
                   AND msg_from = '[hcom-events]'
                   AND json_extract(data, '$.text') LIKE '@' || ? || ' %'",
                params![name],
                |r| r.get(0),
            )
            .unwrap()
    }

    fn stop_for_resume(db: &HcomDb, name: &str) {
        let created_at: f64 = db
            .conn
            .query_row(
                "SELECT created_at FROM instances WHERE name = ?",
                params![name],
                |r| r.get(0),
            )
            .unwrap();
        let event = json!({"action": "stopped", "snapshot": {"name": name}});
        assert!(
            db.finalize_instance_stop(name, created_at, None, None, &event)
                .unwrap()
        );
    }

    #[test]
    fn replay_predicate_reuse_is_bounded_and_rebinds_after_errors() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut cache = ReplayPredicates::new(&conn);
        for pass in 0..2 {
            for i in 0..=REPLAY_PREDICATE_LIMIT {
                let query = format!("SELECT 1 WHERE ?1 = ?2 AND {i} = {i}");
                let values = params![i as i64, (i + pass) as i64];
                let expected = conn.query_row(&query, values, |_| Ok(())).is_ok();
                assert_eq!(
                    cache.matches(&query, params![i as i64, (i + pass) as i64]),
                    expected
                );
            }
            assert_eq!(cache.statements.len(), REPLAY_PREDICATE_LIMIT);
        }
        assert!(
            cache.statements["SELECT 1 WHERE ?1 = ?2 AND 0 = 0"]
                .get_status(rusqlite::StatementStatus::Run)
                >= 2
        );
        let mut cache = ReplayPredicates::new(&conn);
        assert!(!cache.matches("invalid SQL", []));
        assert!(!cache.matches("SELECT ?1 WHERE ?2", params![1]));
        assert!(cache.matches("SELECT ?1 WHERE ?2", params![2, true]));
        assert!(!cache.matches("SELECT ?1 WHERE ?2", params![3, false]));
        let oversized = format!("SELECT 1 /*{}*/", "x".repeat(REPLAY_PREDICATE_SQL_BYTES));
        assert!(cache.matches(&oversized, []));
        assert!(!cache.statements.contains_key(&oversized));
        assert!(cache.sql_bytes <= REPLAY_PREDICATE_SQL_BYTES);
    }

    #[test]
    fn replay_only_parses_admitted_eligible_rows_and_can_match_later_eligible_rows() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute_batch("INSERT INTO instances (name, created_at) VALUES ('mula', 1.0)")
            .unwrap();
        let data = json!({"status": "active"});
        let event_id = db.insert_event_row("status", "libe", &data, None).unwrap();
        kv_store_sub(
            &db,
            "events_sub:a",
            &json!({"id": "a", "caller": "mula", "sql": "0"}),
        );
        kv_store_sub(
            &db,
            "events_sub:b",
            &json!({"id": "b", "caller": "mula", "sql": "type = 'status' AND status_val = 'active'"}),
        );
        assert!(
            subscription_rows(&db, None, None)
                .iter()
                .all(|row| row.sub.is_none())
        );
        let mut replay = ReplayState {
            predicates: ReplayPredicates::new(&db.conn),
            reactions: Vec::new(),
            rows: None,
        };
        let eligible = HashSet::from(["events_sub:a".to_string()]);
        db.with_write_scope(|| {
            let rows = subscription_rows(&db, Some(&eligible), Some(&mut replay));
            assert!(
                rows.iter()
                    .find(|row| row.key == "events_sub:a")
                    .unwrap()
                    .sub
                    .is_some()
            );
            assert!(
                rows.iter()
                    .find(|row| row.key == "events_sub:b")
                    .unwrap()
                    .sub
                    .is_none()
            );
            // Growing eligibility must not reuse a filtered-out row set. The
            // raw row remains available without retaining its parsed form.
            let later = subscription_rows(&db, None, Some(&mut replay));
            assert!(std::sync::Arc::ptr_eq(&rows, &later));
            assert_eq!(later.len(), 2);
            fire_matching_subscriptions(
                &db,
                event_id,
                "status",
                "libe",
                &data,
                None,
                Some(&mut replay),
            );
            assert_eq!(notices_to(&db, "mula"), 1);
            let updated: serde_json::Value =
                serde_json::from_str(&db.kv_get("events_sub:b")?.unwrap()).unwrap();
            assert_eq!(updated["last_id"], event_id);
            Ok(())
        })
        .unwrap();
        drop(replay);
        cleanup_test_db(db_path);
    }

    #[test]
    fn replay_rows_invalidate_on_writes_and_never_survive_autocommit_or_overflow() {
        let (db, db_path) = setup_full_test_db();
        let mut replay = ReplayState {
            predicates: ReplayPredicates::new(&db.conn),
            reactions: Vec::new(),
            rows: None,
        };
        let sub = json!({"id": "a", "caller": "mula", "sql": "0", "last_id": 0});
        kv_store_sub(&db, "events_sub:a", &sub);
        db.with_write_scope(|| {
            let original = subscription_rows(&db, None, Some(&mut replay));
            let reused = subscription_rows(&db, None, Some(&mut replay));
            assert!(std::sync::Arc::ptr_eq(&original, &reused));
            db.conn.execute_batch(
                "CREATE TRIGGER rewrite_subscription AFTER INSERT ON kv
                WHEN NEW.key = 'fixture-change'
                BEGIN UPDATE kv SET value = json_set(value, '$.sql', '1')
                WHERE key = 'events_sub:a'; END;",
            )?;
            db.kv_set("fixture-change", Some("1"))?;
            let changed = subscription_rows(&db, None, Some(&mut replay));
            assert!(!std::sync::Arc::ptr_eq(&original, &changed));
            assert_eq!(changed[0].sub.as_ref().unwrap()["sql"], "1");
            db.kv_set("events_sub:a", None)?;
            assert!(subscription_rows(&db, None, Some(&mut replay)).is_empty());
            Ok(())
        })
        .unwrap();
        // Another connection may write between autocommit reads.
        subscription_rows(&db, None, Some(&mut replay));
        assert!(replay.rows.is_none());
        db.with_write_scope(|| {
            for i in 0..=REPLAY_ROW_LIMIT {
                kv_store_sub(&db, &format!("events_sub:{i}"), &sub);
            }
            assert_eq!(
                subscription_rows(&db, None, Some(&mut replay)).len(),
                REPLAY_ROW_LIMIT + 1
            );
            assert!(replay.rows.is_none());
            assert!(
                subscription_rows(&db, None, Some(&mut replay))
                    .iter()
                    .all(|row| row.sub.is_none())
            );
            db.kv_delete_prefix("events_sub:")?;
            let oversized = json!({"padding": "x".repeat(REPLAY_ROW_BYTES)});
            kv_store_sub(&db, "events_sub:large", &oversized);
            assert_eq!(subscription_rows(&db, None, Some(&mut replay)).len(), 1);
            assert!(replay.rows.is_none());
            // A failed row read must not memoize a transient/partial result.
            db.kv_delete_prefix("events_sub:")?;
            db.kv_set("events_sub:invalid", Some("not JSON"))?;
            subscription_rows(&db, None, Some(&mut replay));
            assert!(replay.rows.is_none());
            Ok(())
        })
        .unwrap();
        drop(replay);
        cleanup_test_db(db_path);
    }

    #[test]
    fn replay_observes_filter_rewrites_from_notification_triggers() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute_batch(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 1.0), ('libe', 1.0)",
            )
            .unwrap();
        for (id, sql, once) in [
            ("a", "type = 'status' AND status_val = 'listening'", false),
            ("b", "0", true),
        ] {
            kv_store_sub(
                &db,
                &format!("events_sub:{id}"),
                &json!({"id": id, "caller": "mula", "sql": sql, "last_id": 0, "once": once}),
            );
        }
        stop_for_resume(&db, "mula");
        db.log_status_event("libe", "listening", "", None, None)
            .unwrap();
        let first_hit = db.get_last_event_id();
        db.log_status_event("libe", "active", "", None, None)
            .unwrap();
        db.conn.execute_batch("INSERT INTO instances (name, created_at) VALUES ('mula', 2.0);
            CREATE TRIGGER rewrite_filter AFTER INSERT ON kv
            WHEN NEW.key = 'events_sub:a' AND json_extract(NEW.value, '$.last_id') > 0
            BEGIN UPDATE kv SET value = json_set(value, '$.sql', 'type = ''status'' AND status_val = ''active''')
            WHERE key = 'events_sub:b'; END;").unwrap();
        db.with_write_scope(|| restore_stopped_subscriptions(&db, "mula"))
            .unwrap();
        assert_eq!(notices_to(&db, "mula"), 2);
        assert!(db.kv_get("events_sub:b").unwrap().is_none());
        let sub: serde_json::Value =
            serde_json::from_str(&db.kv_get("events_sub:a").unwrap().unwrap()).unwrap();
        assert_eq!(sub["last_id"], first_hit);
        cleanup_test_db(db_path);
    }

    #[test]
    fn replay_parameterized_predicates_keep_bindings_once_and_cursors() {
        let (db, db_path) = setup_full_test_db();
        db.conn.execute_batch("INSERT INTO instances (name, created_at) VALUES ('mula', 1.0), ('libe', 1.0), ('nabe', 1.0)").unwrap();
        for (id, watched, once) in [("a", "libe", false), ("b", "nabe", true)] {
            let sub = json!({"id": id, "caller": "mula", "once": once,
                "sql": "type = 'status' AND instance = ? AND status_val = ?",
                "params": [watched, "listening"], "last_id": 0});
            db.kv_set(&format!("events_sub:{id}"), Some(&sub.to_string()))
                .unwrap();
        }
        stop_for_resume(&db, "mula");
        db.log_status_event("libe", "listening", "", None, None)
            .unwrap();
        db.log_status_event("nabe", "listening", "", None, None)
            .unwrap();
        db.log_status_event("libe", "listening", "", None, None)
            .unwrap();
        let last_libe = db.get_last_event_id();
        db.log_status_event("nabe", "listening", "", None, None)
            .unwrap();
        db.log_status_event("libe", "active", "", None, None)
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 2.0)",
                [],
            )
            .unwrap();
        db.with_write_scope(|| restore_stopped_subscriptions(&db, "mula"))
            .unwrap();
        assert_eq!(
            notices_to(&db, "mula"),
            3,
            "two libe hits and only the first nabe hit"
        );
        assert!(db.kv_get("events_sub:b").unwrap().is_none());
        let sub: serde_json::Value =
            serde_json::from_str(&db.kv_get("events_sub:a").unwrap().unwrap()).unwrap();
        assert_eq!(sub["last_id"], last_libe);
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_resume_restores_once_sub_and_replays_missed_match() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 1.0), ('libe', 1.0)",
                [],
            )
            .unwrap();
        let sub = json!({
            "id": "sub-idle", "caller": "mula", "once": true,
            "sql": "type = 'status' AND instance = 'libe' AND status_val = 'listening'",
            "last_id": db.get_last_event_id(),
        });
        db.kv_set("events_sub:sub-idle", Some(&sub.to_string()))
            .unwrap();

        stop_for_resume(&db, "mula");
        assert!(db.kv_get("events_sub:sub-idle").unwrap().is_none());

        // The match happens while mula is stopped.
        db.log_status_event("libe", "listening", "", None, None)
            .unwrap();
        assert_eq!(notices_to(&db, "mula"), 0);

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 2.0)",
                [],
            )
            .unwrap();
        assert_eq!(restore_stopped_subscriptions(&db, "mula").unwrap(), 1);
        assert_eq!(
            notices_to(&db, "mula"),
            1,
            "missed match notifies on resume"
        );
        assert!(
            db.kv_get("events_sub:sub-idle").unwrap().is_none(),
            "once sub is consumed by the replayed match"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_resume_restores_continuous_sub_without_refiring_old_matches() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 1.0), ('libe', 1.0)",
                [],
            )
            .unwrap();
        let sub = json!({
            "id": "sub-watch", "caller": "mula",
            "sql": "type = 'status' AND instance = 'libe' AND status_val = 'listening'",
            "last_id": db.get_last_event_id(),
        });
        db.kv_set("events_sub:sub-watch", Some(&sub.to_string()))
            .unwrap();

        // Matched (and notified) while mula was alive.
        db.log_status_event("libe", "listening", "", None, None)
            .unwrap();
        assert_eq!(notices_to(&db, "mula"), 1);

        stop_for_resume(&db, "mula");
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 2.0)",
                [],
            )
            .unwrap();
        db.with_write_scope(|| restore_stopped_subscriptions(&db, "mula"))
            .unwrap();
        assert_eq!(
            notices_to(&db, "mula"),
            1,
            "no replay of an already-notified match"
        );
        assert!(db.kv_get("events_sub:sub-watch").unwrap().is_some());

        // Still live after restore.
        db.log_status_event("libe", "listening", "", None, None)
            .unwrap();
        assert_eq!(notices_to(&db, "mula"), 2);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_resume_does_not_rematch_events_seen_live_before_stop() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 1.0), ('libe', 1.0)",
                [],
            )
            .unwrap();
        // Like the collision preset: a libe event matches only once a later
        // event exists, so live matching (at log time) never hits it and
        // never moves last_id.
        let sub = json!({
            "id": "sub-later", "caller": "mula",
            "sql": "type = 'status' AND instance = 'libe' AND EXISTS (
                      SELECT 1 FROM events_v e
                      WHERE e.id > events_v.id AND e.instance = 'nemo')",
            "last_id": db.get_last_event_id(),
        });
        db.kv_set("events_sub:sub-later", Some(&sub.to_string()))
            .unwrap();
        db.log_status_event("libe", "listening", "", None, None)
            .unwrap();
        db.log_status_event("nemo", "listening", "", None, None)
            .unwrap();
        assert_eq!(notices_to(&db, "mula"), 0);

        stop_for_resume(&db, "mula");
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 2.0)",
                [],
            )
            .unwrap();
        assert_eq!(restore_stopped_subscriptions(&db, "mula").unwrap(), 1);
        assert_eq!(
            notices_to(&db, "mula"),
            0,
            "events seen live before the stop must not be replayed"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_resume_restores_thread_membership_dropped_by_name_reuse_cleanup() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 1.0)",
                [],
            )
            .unwrap();
        let member = json!({
            "id": "sub-thread", "caller": "mula", "thread_name": "uvx",
            "auto_thread_member": true, "delivery_only": true, "last_id": 0,
        });
        db.kv_set("events_sub:sub-thread", Some(&member.to_string()))
            .unwrap();

        stop_for_resume(&db, "mula");
        // Re-creating the row runs the name-reuse cleanup.
        db.cleanup_thread_memberships_for_name_reuse("mula")
            .unwrap();
        assert!(db.kv_get("events_sub:sub-thread").unwrap().is_none());

        assert_eq!(restore_stopped_subscriptions(&db, "mula").unwrap(), 1);
        assert!(db.kv_get("events_sub:sub-thread").unwrap().is_some());

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_resume_restores_subs_deleted_by_earlier_soft_stop_of_same_row() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 1.0)",
                [],
            )
            .unwrap();
        let sub = json!({"id": "sub-watch", "caller": "mula", "sql": "0", "last_id": 0});
        db.kv_set("events_sub:sub-watch", Some(&sub.to_string()))
            .unwrap();

        // Soft stop (as soft_finalize_session): save, delete, keep the row.
        let saved = subscriptions_to_save(&db, "mula", 1.0).unwrap();
        db.cleanup_subscriptions("mula").unwrap();
        db.log_event(
            "life",
            "mula",
            &json!({"action": "stopped", "snapshot": {"created_at": 1.0, "subscriptions": saved}}),
        )
        .unwrap();

        // The row's hard stop finds nothing owned but carries the soft save.
        stop_for_resume(&db, "mula");
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 2.0)",
                [],
            )
            .unwrap();
        assert_eq!(restore_stopped_subscriptions(&db, "mula").unwrap(), 1);
        assert!(db.kv_get("events_sub:sub-watch").unwrap().is_some());

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_resume_replay_skips_events_before_each_subs_cursor() {
        let (db, db_path) = setup_full_test_db();
        // An older sub makes replay start early.
        let old = json!({"id": "sub-old", "caller": "gora", "sql": "0", "last_id": 0});
        db.kv_set("events_sub:sub-old", Some(&old.to_string()))
            .unwrap();
        // nabe messages gora before gora's request exists.
        db.log_event(
            "message",
            "nabe",
            &json!({"from": "nabe", "sender_kind": "instance", "scope": "mentions",
                    "text": "fyi", "mentions": ["gora"], "delivered_to": ["gora"]}),
        )
        .unwrap();
        let request_id = setup_reqwatch_pair(&db, "gora", "nabe", "claude");
        let sub_key = format!("events_sub:reqwatch-{request_id}-nabe");

        stop_for_resume(&db, "gora");
        db.conn
            .execute(
                "INSERT INTO instances (name, tool, created_at) VALUES ('gora', 'claude', 2.0)",
                [],
            )
            .unwrap();
        db.with_write_scope(|| restore_stopped_subscriptions(&db, "gora"))
            .unwrap();

        assert!(
            db.kv_get(&sub_key).unwrap().is_some(),
            "a message older than the request must not cancel its watch"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_resume_replay_defers_on_hit_until_history_is_replayed() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 1.0), ('libe', 1.0)",
                [],
            )
            .unwrap();
        let last = db.get_last_event_id();
        // A reacts to libe idling; B watches every message.
        let a = json!({"id": "sub-a", "caller": "mula", "last_id": last,
            "sql": "type = 'status' AND instance = 'libe' AND status_val = 'listening'",
            "on_hit_text": "libe idle", "caller_kind": "instance"});
        let b = json!({"id": "sub-b", "caller": "mula", "last_id": last,
            "sql": "type = 'message'"});
        db.kv_set("events_sub:sub-a", Some(&a.to_string())).unwrap();
        db.kv_set("events_sub:sub-b", Some(&b.to_string())).unwrap();
        stop_for_resume(&db, "mula");

        db.log_status_event("libe", "listening", "", None, None)
            .unwrap();
        let missed = db
            .log_event(
                "message",
                "libe",
                &json!({"from": "libe", "sender_kind": "instance", "scope": "broadcast",
                        "text": "done"}),
            )
            .unwrap();

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('mula', 2.0)",
                [],
            )
            .unwrap();
        db.with_write_scope(|| restore_stopped_subscriptions(&db, "mula"))
            .unwrap();

        let notified: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events_v WHERE type = 'message'
                   AND msg_from = '[hcom-events]'
                   AND json_extract(data, '$.text') LIKE '%sub-b%'
                   AND json_extract(data, '$.text') LIKE '%#' || ? || ' %'",
                params![missed],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            notified, 1,
            "B's missed message survives A's on-hit reaction"
        );

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_resume_replay_resets_agy_grace_before_matching() {
        let (db, db_path) = setup_full_test_db();
        let request_id = setup_reqwatch_pair(&db, "gora", "nabe", "antigravity");
        let sub_key = format!("events_sub:reqwatch-{request_id}-nabe");
        let before = count_reqwatch_without_reply_notifications(&db, "gora");

        // Grace armed by an idle spell, then expired before gora stops.
        db.log_event(
            "status",
            "nabe",
            &json!({"status": "listening", "context": ""}),
        )
        .unwrap();
        let mut sub: serde_json::Value =
            serde_json::from_str(&db.kv_get(&sub_key).unwrap().unwrap()).unwrap();
        sub["idle_grace_until"] = json!(1.0);
        kv_store_sub(&db, &sub_key, &sub);
        stop_for_resume(&db, "gora");

        // While gora is stopped nabe works again, then starts a new idle spell.
        db.log_event(
            "status",
            "nabe",
            &json!({"status": "active", "context": "tool:x"}),
        )
        .unwrap();
        db.log_event(
            "status",
            "nabe",
            &json!({"status": "listening", "context": ""}),
        )
        .unwrap();

        db.conn
            .execute(
                "INSERT INTO instances (name, tool, created_at) VALUES ('gora', 'claude', 2.0)",
                [],
            )
            .unwrap();
        db.with_write_scope(|| restore_stopped_subscriptions(&db, "gora"))
            .unwrap();

        assert_eq!(
            count_reqwatch_without_reply_notifications(&db, "gora"),
            before,
            "the new idle spell gets a fresh grace, not the stale expired one"
        );
        let rearmed: serde_json::Value =
            serde_json::from_str(&db.kv_get(&sub_key).unwrap().unwrap()).unwrap();
        assert!(rearmed["idle_grace_until"].as_f64().unwrap() > 1.0);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_get_thread_members_filters_stale_names() {
        let (db, db_path) = setup_full_test_db();

        db.conn
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0), ('nova', 1000.0)",
                [],
            )
            .unwrap();

        db.add_thread_memberships(
            "debate-1",
            Some("luna"),
            &["nova".to_string(), "ghost".to_string()],
        );

        let stored: String = db
            .conn
            .query_row(
                "SELECT value FROM kv WHERE key = ?",
                params![format!(
                    "events_sub:{}",
                    thread_membership_sub_id("debate-1", "luna")
                )],
                |row| row.get(0),
            )
            .unwrap();
        assert!(stored.contains("\"sql\":\"0\""));

        assert_eq!(
            db.get_thread_members("debate-1"),
            vec!["nova".to_string(), "luna".to_string()]
        );

        cleanup_test_db(db_path);
    }
}
