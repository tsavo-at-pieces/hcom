//! GitHub Copilot CLI native hook handlers and hooks/hcom.json management.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde_json::{Value, json};

use crate::db::{HcomDb, InstanceRow};
use crate::hooks::{DeliveryAck, HookPayload, common};
use crate::instance_binding;
use crate::instance_lifecycle as lifecycle;
use crate::instances;
use crate::log;
use crate::paths;
use crate::shared::context::HcomContext;
use crate::shared::{ST_ACTIVE, ST_LISTENING};

use super::runtime::{self, LaunchCtx, PerRunAdapter, RuntimeInjection};

const HCOM_TRIGGER: &str = "<hcom>";
const HOOK_TIMEOUT_SECS: u64 = 15;
// PascalCase selects Copilot's VS Code-compatible payload format; changing to
// camelCase also changes payload fields, so it requires checking
// HookPayload::from_copilot_native, not just renaming entries in this table.
// `command` is the documented cross-platform fallback; explicit `bash` and
// `powershell` fields take precedence on their respective platforms.
// https://docs.github.com/en/copilot/reference/hooks-reference
//
// Keep Notification, PermissionRequest and PostToolUseFailure: their handlers
// support idle delivery, approval detection and tool-failure status. These CLI
// events are documented; the cloud agent supports a smaller event set.
const COPILOT_HOOK_COMMANDS: &[(&str, &str, bool, Option<&str>)] = &[
    ("SessionStart", "copilot-sessionstart", false, None),
    ("UserPromptSubmit", "copilot-userpromptsubmit", false, None),
    ("PreToolUse", "copilot-pretooluse", false, None),
    ("PermissionRequest", "copilot-permissionrequest", true, None),
    ("PostToolUse", "copilot-posttooluse", false, None),
    ("ErrorOccurred", "copilot-erroroccurred", false, None),
    (
        "PostToolUseFailure",
        "copilot-posttoolusefailure",
        false,
        None,
    ),
    (
        "Notification",
        "copilot-notification",
        false,
        Some("agent_idle|permission_prompt"),
    ),
    ("Stop", "copilot-agentstop", false, None),
    ("SubagentStart", "copilot-subagentstart", false, None),
    ("SubagentStop", "copilot-subagentstop", false, None),
    ("SessionEnd", "copilot-sessionend", false, None),
];

pub static PER_RUN: PerRunAdapter = PerRunAdapter {
    prepare: prepare_per_run,
    cleanup_legacy: cleanup_legacy_per_run,
    ensure_permissions: None,
    managed_value_flags: &["--plugin-dir"],
    strip_legacy_args: None,
};

const COPILOT_PLUGIN_MANIFEST: &[u8] =
    br#"{"name":"hcom","description":"hcom per-run hooks","hooks":"hooks.json"}"#;
const COPILOT_PLUGIN_MANIFEST_WITH_EXTENSION: &[u8] = br#"{"name":"hcom","description":"hcom per-run hooks","hooks":"hooks.json","extensions":"extensions"}"#;
const EXTENSION_SOURCE: &str = include_str!("../copilot_plugin/extension.mjs");
const EXTENSION_PATH: &str = "extensions/hcom/extension.mjs";

/// Set on a launch whose idle delivery belongs to the extension; the PTY
/// delivery loop reads it and never injects.
pub const EXTENSION_DELIVERY_ENV: &str = "HCOM_COPILOT_EXT_DELIVERY";

/// Whether this PTY process was launched with extension delivery.
pub fn extension_delivery_enabled() -> bool {
    std::env::var(EXTENSION_DELIVERY_ENV).is_ok_and(|v| v == "1")
}

/// The extension invokes hcom the same way the hooks do.
fn extension_source() -> Result<String> {
    let prefix = serde_json::to_string(&crate::runtime_env::get_hcom_prefix())?;
    Ok(EXTENSION_SOURCE.replace("__HCOM_PREFIX__", &prefix))
}

/// `copilot_delivery = "native"`, resolved with the launch env (HCOM_* overrides).
fn native_delivery_configured(ctx: &LaunchCtx) -> bool {
    crate::config::HcomConfig::load(Some(&ctx.env)).copilot_delivery == "native"
}

/// Copilot's settings.json for this launch's `COPILOT_HOME`.
fn copilot_home(ctx: &LaunchCtx) -> PathBuf {
    ctx.path_var("COPILOT_HOME")
        .unwrap_or_else(|| ctx.home().join(".copilot"))
}

#[derive(Debug, thiserror::Error)]
pub enum SetupError {
    #[error("existing Copilot hook file at {} could not be read: {source}", path.display())]
    ExistingReadFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("existing Copilot hook file at {} is not valid JSON: {source}", path.display())]
    ExistingParseFailed {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("existing Copilot hook file at {} must be a JSON object", path.display())]
    ExistingRootNotObject { path: PathBuf },
    #[error("failed to create Copilot hook directory {}: {source}", path.display())]
    DirCreateFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("JSON serialization failed: {0}")]
    SerializationFailed(#[from] serde_json::Error),
    #[error("atomic write to {} failed: {source}", path.display())]
    AtomicWriteFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn hcom_hooks_file(copilot_home: PathBuf) -> PathBuf {
    copilot_home.join("hooks").join("hcom.json")
}

fn copilot_hooks_path_for_ctx(ctx: &LaunchCtx) -> PathBuf {
    hcom_hooks_file(
        ctx.path_var("COPILOT_HOME")
            .unwrap_or_else(|| ctx.home().join(".copilot")),
    )
}

/// Older hcom used `<HCOM_DIR parent>/.copilot` under a project-local HCOM_DIR.
fn project_local_legacy_path() -> Option<PathBuf> {
    crate::runtime_env::legacy_tool_config_root().map(|root| hcom_hooks_file(root.join(".copilot")))
}

/// Relative paths (e.g. a relative `COPILOT_HOME`) resolve against the
/// current dir, where the tool run from here would resolve them.
fn push_unique(paths: &mut Vec<PathBuf>, path: PathBuf) {
    let path = std::env::current_dir()
        .map(|cwd| cwd.join(&path))
        .unwrap_or(path);
    if path.is_absolute() && !paths.contains(&path) {
        paths.push(path);
    }
}

fn copilot_hooks_cleanup_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for dir in crate::runtime_env::tool_config_cleanup_dirs(".copilot", "COPILOT_HOME") {
        push_unique(&mut paths, hcom_hooks_file(dir));
    }
    paths
}

fn build_copilot_hook_command(command: &str) -> String {
    let mut parts = crate::runtime_env::get_hcom_prefix();
    parts.push(command.to_string());
    parts.join(" ")
}

fn is_hcom_copilot_command(command: &str) -> bool {
    COPILOT_HOOK_COMMANDS.iter().any(|(_, suffix, _, _)| {
        command == build_copilot_hook_command(suffix) || command.ends_with(suffix)
    })
}

fn expected_hook(command: &str, matcher: Option<&str>) -> Value {
    let mut obj = serde_json::Map::from_iter([
        ("type".to_string(), Value::String("command".to_string())),
        (
            "command".to_string(),
            Value::String(build_copilot_hook_command(command)),
        ),
        ("timeoutSec".to_string(), json!(HOOK_TIMEOUT_SECS)),
    ]);
    if let Some(matcher) = matcher {
        obj.insert("matcher".to_string(), Value::String(matcher.to_string()));
    }
    Value::Object(obj)
}

fn read_json_object(path: &Path) -> Result<serde_json::Map<String, Value>, SetupError> {
    if !path.exists() {
        return Ok(serde_json::Map::new());
    }
    let content =
        std::fs::read_to_string(path).map_err(|source| SetupError::ExistingReadFailed {
            path: path.to_path_buf(),
            source,
        })?;
    let value = serde_json::from_str::<Value>(&content).map_err(|source| {
        SetupError::ExistingParseFailed {
            path: path.to_path_buf(),
            source,
        }
    })?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| SetupError::ExistingRootNotObject {
            path: path.to_path_buf(),
        })
}

fn write_json(path: &Path, value: &Value) -> Result<(), SetupError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| SetupError::DirCreateFailed {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let content = serde_json::to_string_pretty(value)?;
    paths::atomic_write_io(path, &content).map_err(|source| SetupError::AtomicWriteFailed {
        path: path.to_path_buf(),
        source,
    })
}

fn merge_hcom_hooks(root: &mut Value, include_permissions: bool) {
    if !root.is_object() {
        *root = json!({});
    }
    let obj = root.as_object_mut().unwrap();
    obj.entry("version".to_string()).or_insert_with(|| json!(1));
    let hooks = obj.entry("hooks".to_string()).or_insert_with(|| json!({}));
    if !hooks.is_object() {
        *hooks = json!({});
    }
    let hooks = hooks.as_object_mut().unwrap();

    for entries in hooks.values_mut() {
        if let Some(entries) = entries.as_array_mut() {
            entries.retain(|entry| {
                !entry
                    .get("command")
                    .and_then(Value::as_str)
                    .is_some_and(is_hcom_copilot_command)
            });
        }
    }

    for (event, command, permissions_only, matcher) in COPILOT_HOOK_COMMANDS {
        if *permissions_only && !include_permissions {
            continue;
        }
        let entries = hooks
            .entry((*event).to_string())
            .or_insert_with(|| json!([]));
        if !entries.is_array() {
            *entries = json!([]);
        }
        entries
            .as_array_mut()
            .unwrap()
            .push(expected_hook(command, *matcher));
    }
    hooks.retain(|_, entries| {
        entries
            .as_array()
            .is_some_and(|entries| !entries.is_empty())
    });
}

fn remove_hcom_hooks(root: &mut Value) {
    let Some(hooks) = root.get_mut("hooks").and_then(Value::as_object_mut) else {
        return;
    };
    for entries in hooks.values_mut() {
        let Some(entries) = entries.as_array_mut() else {
            continue;
        };
        entries.retain(|entry| {
            !entry
                .get("command")
                .and_then(Value::as_str)
                .is_some_and(is_hcom_copilot_command)
        });
    }
    hooks.retain(|_, entries| {
        entries
            .as_array()
            .is_some_and(|entries| !entries.is_empty())
    });
}

fn runtime_hooks_json(include_permissions: bool) -> Result<Vec<u8>> {
    let mut root = json!({});
    merge_hcom_hooks(&mut root, include_permissions);
    Ok(serde_json::to_vec_pretty(&root)?)
}

fn prepare_per_run(ctx: &LaunchCtx) -> Result<RuntimeInjection> {
    let hooks = runtime_hooks_json(ctx.auto_approve)?;
    // `copilot_delivery = "native"`: Copilot is made to load the extension,
    // and failing that the launch fails rather than silently degrading to
    // terminal delivery.
    let with_extension = native_delivery_configured(ctx);
    if with_extension {
        crate::tools::copilot_preprocessing::ensure_copilot_extensions_enabled(&copilot_home(ctx))
            .context("copilot_delivery is native (hcom config copilot_delivery inject to use terminal delivery)")?;
    }
    let extension = if with_extension {
        extension_source()?
    } else {
        String::new()
    };
    let mut files = vec![("hooks.json", hooks.as_slice())];
    let mut env = Vec::new();
    if with_extension {
        files.push(("plugin.json", COPILOT_PLUGIN_MANIFEST_WITH_EXTENSION));
        files.push((EXTENSION_PATH, extension.as_bytes()));
        env.push((EXTENSION_DELIVERY_ENV.to_string(), "1".to_string()));
    } else {
        files.push(("plugin.json", COPILOT_PLUGIN_MANIFEST));
    }
    let plugin_dir =
        runtime::publish_dir("copilot", &files).context("Cannot publish Copilot runtime plugin")?;
    let mut args = ctx.args.clone();
    runtime::insert_before_separator(
        &mut args,
        [
            "--plugin-dir".to_string(),
            plugin_dir.to_string_lossy().into_owned(),
        ],
    );
    Ok(RuntimeInjection { args, env })
}

fn cleanup_legacy_per_run(ctx: &LaunchCtx) -> Result<()> {
    let effective = copilot_hooks_path_for_ctx(ctx);
    let legacy = project_local_legacy_path().filter(|path| *path != effective);
    runtime::collect_errors(
        std::iter::once(effective)
            .chain(legacy)
            .filter_map(|path| remove_hooks_at(&path).err())
            .collect(),
    )
}

/// Strip hcom's entries from a legacy `hooks/hcom.json`. When that removed
/// something and only `version` and empty `hooks` remain, the file is deleted;
/// a file hcom had no entries in is never touched. Returns whether anything
/// was removed.
fn remove_hooks_at(path: &Path) -> Result<bool> {
    if !crate::runtime_env::hook_cleanup_allowed(path) {
        return Ok(false);
    }
    if !path.exists() {
        return Ok(false);
    }
    let fix = runtime::FIX_REMOVE_HCOM_HOOKS;
    let mut value = Value::Object(
        read_json_object(path).with_context(|| runtime::LegacyFile::read(path, fix))?,
    );
    let before = value.clone();
    remove_hcom_hooks(&mut value);
    if value == before {
        return Ok(false);
    }
    let only_hcom = value.as_object().is_some_and(|root| {
        root.iter().all(|(key, v)| match key.as_str() {
            "version" => true,
            "hooks" => v.as_object().is_some_and(|hooks| hooks.is_empty()),
            _ => false,
        })
    });
    if only_hcom {
        std::fs::remove_file(path).map_err(anyhow::Error::from)
    } else {
        write_json(path, &value).map_err(anyhow::Error::from)
    }
    .with_context(|| runtime::LegacyFile::write(path, fix))?;
    Ok(true)
}

/// Clean every path the old installer could have used; one failure doesn't
/// stop the others.
pub fn remove_copilot_hooks() -> Result<bool> {
    runtime::sweep(copilot_hooks_cleanup_paths(), |path| remove_hooks_at(&path))
}

fn resolve_instance(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> Option<InstanceRow> {
    instance_binding::resolve_instance_from_binding(
        db,
        payload.session_id.as_deref(),
        ctx.process_id.as_deref(),
    )
}

fn update_position(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload, instance_name: &str) {
    let mut updates = serde_json::Map::new();
    if let Some(session_id) = payload.session_id.as_ref().filter(|s| !s.is_empty()) {
        updates.insert("session_id".into(), Value::String(session_id.clone()));
    }
    if let Some(path) = payload.transcript_path.as_ref().filter(|s| !s.is_empty()) {
        updates.insert("transcript_path".into(), Value::String(path.clone()));
    }
    let cwd = payload
        .raw
        .get("cwd")
        .and_then(Value::as_str)
        .unwrap_or_else(|| ctx.cwd.to_str().unwrap_or(""));
    if !cwd.is_empty() {
        updates.insert("directory".into(), Value::String(cwd.to_string()));
    }
    instances::update_instance_position(db, instance_name, &updates);
}

fn handle_sessionstart(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> Value {
    let Some(session_id) = payload.session_id.as_deref().filter(|sid| !sid.is_empty()) else {
        return json!({});
    };
    let instance_name = ctx
        .process_id
        .as_deref()
        .and_then(|pid| instance_binding::bind_session_to_process(db, session_id, Some(pid)))
        .or_else(|| resolve_instance(db, ctx, payload).map(|instance| instance.name));
    let Some(instance_name) = instance_name else {
        return json!({});
    };
    let _ = db.rebind_instance_session(&instance_name, session_id);
    instance_binding::capture_and_store_launch_context(db, &instance_name);
    let Some(instance) = db.get_instance_full(&instance_name).ok().flatten() else {
        return json!({});
    };
    update_position(db, ctx, payload, &instance_name);
    // Copilot creates the session lazily: SessionStart arrives right after the
    // first UserPromptSubmit, inside that turn. Marking it listening there would
    // invite idle delivery into a busy agent.
    if instance.status != ST_ACTIVE {
        lifecycle::set_status(
            db,
            &instance_name,
            ST_LISTENING,
            "start",
            Default::default(),
        );
    }
    crate::runtime_env::set_terminal_title(&instance_name);
    crate::relay::worker::try_spawn_worker();
    common::notify_hook_instance_with_db(db, &instance_name);
    if let Some(bootstrap) =
        common::inject_bootstrap_once(db, ctx, &instance_name, &instance, "copilot")
    {
        json!({ "additionalContext": bootstrap })
    } else {
        json!({})
    }
}

/// The hook's instance, with its position updated from the hook unless the
/// hook comes from a subagent's session, which must not rebind it.
fn resolved_instance(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> Option<InstanceRow> {
    let instance = resolve_instance(db, ctx, payload)?;
    if !from_subagent_session(payload, &instance) {
        update_position(db, ctx, payload, &instance.name);
    }
    Some(instance)
}

fn handle_userpromptsubmit(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> Value {
    if let Some(instance) = resolved_instance(db, ctx, payload)
        && !from_subagent_session(payload, &instance)
    {
        let prompt = payload
            .raw
            .get("prompt")
            .or_else(|| payload.raw.get("initial_prompt"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let prompt = prompt.trim();
        // The extension's delivery turn: the ack sets `deliver:<sender>`.
        if prompt.starts_with(HCOM_TRIGGER) && ack_ext_delivery(db, &instance.name, prompt) {
            return json!({});
        }
        let context = if prompt == HCOM_TRIGGER {
            "trigger"
        } else {
            "prompt"
        };
        lifecycle::set_status(db, &instance.name, ST_ACTIVE, context, Default::default());
    }
    json!({})
}

fn handle_pretooluse(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> Value {
    if let Some(instance) = resolved_instance(db, ctx, payload) {
        common::update_tool_status(
            db,
            &instance.name,
            "copilot",
            &payload.tool_name,
            &payload.tool_input,
        );
    }
    json!({})
}

/// Pending messages for the hooks to deliver, unless the extension already
/// holds them: a batch it handed to `session.send` just as the user's own
/// prompt won the race to the idle session is queued in Copilot and runs
/// after this turn, so delivering it here too would show it twice.
fn hook_pending_messages(db: &HcomDb, instance: &InstanceRow) -> Option<common::PreparedDelivery> {
    // The instance's own session, not the payload's: a subagent's hooks run in
    // a session of their own.
    if let Some(inflight) = ExtInflight::live(db, instance, None) {
        log::log_info(
            "hooks",
            "copilot.ext_inflight_deferred",
            &format!(
                "instance={} inflight={}; queued delivery prompt will carry it",
                instance.name, inflight.last_event_id
            ),
        );
        return None;
    }
    common::prepare_pending_messages(db, &instance.name)
}

fn pending_additional_context(db: &HcomDb, instance: &InstanceRow) -> (Value, Option<DeliveryAck>) {
    match hook_pending_messages(db, instance) {
        Some(prepared) => (
            json!({ "additionalContext": prepared.formatted }),
            Some(prepared.ack),
        ),
        None => (json!({}), None),
    }
}

fn handle_posttooluse(
    db: &HcomDb,
    ctx: &HcomContext,
    payload: &HookPayload,
) -> (Value, Option<DeliveryAck>) {
    let Some(instance) = resolved_instance(db, ctx, payload) else {
        return (json!({}), None);
    };
    pending_additional_context(db, &instance)
}

/// A hook from a session other than the instance's own: a Copilot subagent
/// (`task`, `explore`, ...) runs in a session of its own and fires its own
/// UserPromptSubmit and Stop, which must not drive the agent's status.
fn from_subagent_session(payload: &HookPayload, instance: &InstanceRow) -> bool {
    matches!(
        (payload.session_id.as_deref(), instance.session_id.as_deref()),
        (Some(incoming), Some(bound)) if !incoming.is_empty() && incoming != bound
    )
}

/// Stop. Pending messages continue the turn; the instance is only marked
/// listening when nothing is handed over, so idle delivery never sees
/// "listening" with the same unread batch this hook is about to deliver.
/// A subagent's Stop neither idles the agent nor takes its messages, which
/// the agent's own hooks deliver once the subagent returns.
fn handle_agentstop(
    db: &HcomDb,
    ctx: &HcomContext,
    payload: &HookPayload,
) -> (Value, Option<DeliveryAck>) {
    let Some(instance) = resolved_instance(db, ctx, payload) else {
        return (json!({ "decision": "allow" }), None);
    };
    if from_subagent_session(payload, &instance) {
        return (json!({ "decision": "allow" }), None);
    }
    if let Some(prepared) = hook_pending_messages(db, &instance) {
        return (
            json!({ "decision": "block", "reason": prepared.formatted }),
            Some(prepared.ack),
        );
    }
    lifecycle::set_status(db, &instance.name, ST_LISTENING, "", Default::default());
    // Every endpoint, not just hook waiters: a message that arrived after the
    // pending check found the instance active, and its wake was declined;
    // idle delivery must look again now.
    crate::notify::wake(db, &instance.name, &[]);
    (json!({ "decision": "allow" }), None)
}

fn handle_notification(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> Value {
    let Some(instance) = resolved_instance(db, ctx, payload) else {
        return json!({});
    };
    match payload.notification_type.as_deref() {
        Some("permission_prompt") => {
            lifecycle::set_status(
                db,
                &instance.name,
                "blocked",
                "approval",
                Default::default(),
            );
            json!({})
        }
        _ => json!({}),
    }
}

fn handle_erroroccurred(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> Value {
    let Some(instance) = resolved_instance(db, ctx, payload) else {
        return json!({});
    };
    let context = payload
        .raw
        .get("error_context")
        .or_else(|| payload.raw.get("errorContext"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let recoverable = payload.raw.get("recoverable").and_then(Value::as_bool);
    let error_name = payload
        .raw
        .pointer("/error/name")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    log::log_warn(
        "hooks",
        "copilot.error_occurred",
        &format!(
            "instance={} context={context} error={error_name} recoverable={recoverable:?}",
            instance.name
        ),
    );
    json!({})
}

fn command_looks_safe_hcom(command: &str) -> bool {
    common::is_safe_hcom_command(command)
}

fn handle_permissionrequest(_db: &HcomDb, _ctx: &HcomContext, payload: &HookPayload) -> Value {
    let command = payload
        .tool_input
        .get("command")
        .or_else(|| payload.tool_input.get("cmd"))
        .or_else(|| payload.tool_input.get("script"))
        .and_then(Value::as_str)
        .unwrap_or("");
    // POSIX shells only: the check parses POSIX quoting, and PowerShell
    // reads a backslash-escaped `;` as a statement separator.
    if (payload.tool_name == "bash" || (payload.tool_name == "shell" && !cfg!(windows)))
        && command_looks_safe_hcom(command)
    {
        json!({ "behavior": "allow", "message": "hcom coordination command" })
    } else {
        json!({})
    }
}

fn handle_subagentstart(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> Value {
    let Some(instance) = resolved_instance(db, ctx, payload) else {
        return json!({});
    };
    let Some(instance_full) = db.get_instance_full(&instance.name).ok().flatten() else {
        return json!({});
    };
    if let Some(bootstrap) =
        common::inject_bootstrap_once(db, ctx, &instance.name, &instance_full, "copilot")
    {
        json!({ "additionalContext": bootstrap })
    } else {
        json!({})
    }
}

/// Only the instance's own session ends it (as in Claude's SessionEnd).
/// Resolution prefers the process binding, so without this check any session
/// the Copilot process ends would stop the instance. After `hcom r`, the new,
/// still-running process sent SessionEnd `user_exit` ~15s in and the resumed
/// instance was stopped. A PTY-launched instance whose session is not bound
/// yet is still cleaned up by the PTY wrapper when Copilot exits.
fn handle_sessionend(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> Value {
    let Some(instance) = resolve_instance(db, ctx, payload) else {
        return json!({});
    };
    let reason = payload
        .raw
        .get("reason")
        .or_else(|| payload.raw.get("stop_reason"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let incoming = payload.session_id.as_deref().filter(|sid| !sid.is_empty());
    if incoming.is_some() && incoming != instance.session_id.as_deref() {
        log::log_warn(
            "hooks",
            "copilot.sessionend_ignored",
            &format!(
                "instance={} incoming_session_id={} bound_session_id={} reason={}",
                instance.name,
                incoming.unwrap_or(""),
                instance.session_id.as_deref().unwrap_or(""),
                reason
            ),
        );
        return json!({});
    }
    // `/clear` ends the session with `user_exit` while the CLI keeps running
    // (hooks reference, SessionEnd). Copilot always runs under hcom's PTY,
    // which finalizes the instance when the CLI really exits, so here only the
    // session is released for the replacement one to bind.
    if reason == "user_exit"
        && instance.tcp_mode != 0
        && let Some(session_id) = incoming
    {
        if let Err(e) = db.release_instance_session(&instance.name, session_id) {
            log::log_warn("hooks", "copilot.session_release_failed", &format!("{e}"));
        }
        // The replacement session starts without hcom's context; its
        // SessionStart (first turn) injects the bootstrap again.
        let mut updates = serde_json::Map::new();
        updates.insert("name_announced".into(), json!(false));
        instances::update_instance_position(db, &instance.name, &updates);
        lifecycle::set_status(
            db,
            &instance.name,
            ST_LISTENING,
            "clear",
            Default::default(),
        );
        log::log_info(
            "hooks",
            "copilot.sessionend_clear",
            &format!(
                "instance={} session_id={session_id}: released (/clear or quit; PTY exit stops the instance)",
                instance.name
            ),
        );
        return json!({});
    }
    update_position(db, ctx, payload, &instance.name);
    common::finalize_session(db, &instance.name, reason, None);
    json!({})
}

fn flag<'a>(argv: &'a [String], name: &str) -> Option<&'a str> {
    argv.iter()
        .position(|arg| arg == name)
        .and_then(|i| argv.get(i + 1))
        .map(String::as_str)
}

fn bound_instance(db: &HcomDb, ctx: &HcomContext) -> Option<InstanceRow> {
    instance_binding::resolve_instance_from_binding(db, None, ctx.process_id.as_deref())
}

/// Extension poll: (re)register its wake port and, only while the instance is
/// idle, hand over pending messages; mid-turn delivery stays with the hooks.
/// Resolved through the process binding on every call so the extension follows
/// renames and session switches without tracking identity itself.
///
/// `--session-id`: the extension's Copilot session. Copilot runs extensions
/// per session, so one attached to a background session must not take this
/// instance's messages (or its wake endpoint). Before SessionStart binds a
/// session, the foreground one at launch is the only candidate.
///
/// `--idle`: Copilot reported `session.idle`. That is authoritative, so it
/// also recovers a turn whose Stop hook never ran (e.g. an interrupt).
fn handle_ext_poll(db: &HcomDb, ctx: &HcomContext, argv: &[String]) -> Value {
    let Some(instance) = bound_instance(db, ctx) else {
        return json!({ "error": "no instance bound to this process" });
    };
    let name = instance.name.as_str();
    if let (Some(sid), Some(bound)) = (flag(argv, "--session-id"), instance.session_id.as_deref())
        && sid != bound
    {
        log::log_warn(
            "hooks",
            "copilot.ext_session_mismatch",
            &format!("instance={name} extension_session={sid} bound_session={bound}"),
        );
        return json!({ "name": name, "skipped": "session_mismatch" });
    }
    if let Some(port) = flag(argv, "--notify-port").and_then(|p| p.parse::<u16>().ok())
        && port > 0
        && db.get_notify_endpoint_port(name, "plugin") != Some(port)
    {
        match db.upsert_notify_endpoint(name, "plugin", port) {
            Ok(()) => log::log_info(
                "hooks",
                "copilot.ext_bound",
                &format!(
                    "instance={name} port={port} session={}",
                    flag(argv, "--session-id").unwrap_or("")
                ),
            ),
            Err(e) => log::log_warn(
                "hooks",
                "copilot.ext_register_failed",
                &format!("instance={name} err={e}"),
            ),
        }
    }
    if argv.iter().any(|a| a == "--idle") && instance.status != ST_LISTENING {
        log::log_info(
            "hooks",
            "copilot.ext_idle",
            &format!(
                "instance={name} status was {}:{}; Copilot reports idle",
                instance.status, instance.status_context
            ),
        );
        lifecycle::set_status(db, name, ST_LISTENING, "", Default::default());
    }
    if !db.is_idle(name) {
        return json!({ "name": name });
    }
    if let Some(inflight) = ExtInflight::live(db, &instance, flag(argv, "--session-id")) {
        return json!({ "name": name, "inflight": inflight.last_event_id });
    }
    let Some(prepared) = common::prepare_pending_messages(db, name) else {
        return json!({ "name": name });
    };
    let inflight = ExtInflight {
        last_event_id: prepared.ack.last_event_id,
        status_context: prepared.ack.status_context,
        msg_ts: prepared.ack.msg_ts,
        text: prepared.formatted,
        sent_at: crate::shared::time::now_epoch_i64(),
        // The extension's own session: before SessionStart binds one (Copilot
        // creates it lazily) the instance has none to record.
        session_id: flag(argv, "--session-id")
            .map(str::to_string)
            .or_else(|| instance.session_id.clone()),
        confirmed_at: None,
    };
    // Without the record nothing could ack this batch, so it is not handed out.
    if let Err(e) = db.kv_set(&ext_inflight_key(name), Some(&json!(inflight).to_string())) {
        return json!({ "error": format!("record in-flight batch: {e}") });
    }
    json!({
        "name": name,
        "count": prepared.messages.len(),
        "event_id": inflight.last_event_id,
        "text": inflight.text,
    })
}

/// The extension's report on the prompt carrying the batch ending at
/// `--event-id`. Without `--failed`: Copilot accepted it and, if it is waiting
/// behind a user turn that won the race to an idle session, still has it
/// queued; the batch belongs to that prompt until UserPromptSubmit. The
/// extension repeats this while the prompt stays queued. `--failed`: the send
/// failed or the prompt left the queue without running; the record is dropped
/// so the next hook or poll delivers the batch again.
fn handle_ext_sent(db: &HcomDb, ctx: &HcomContext, argv: &[String]) -> Value {
    let Some(instance) = bound_instance(db, ctx) else {
        return json!({ "error": "no instance bound to this process" });
    };
    let name = instance.name.as_str();
    let event_id = flag(argv, "--event-id").and_then(|id| id.parse::<i64>().ok());
    let session = flag(argv, "--session-id");
    // Only the session that was handed the batch speaks for it: a late report
    // from the extension of a session since cleared must not touch a record
    // the new session's extension owns.
    let Some(mut inflight) = ExtInflight::load(db, name).filter(|inflight| {
        Some(inflight.last_event_id) == event_id && inflight.session_id.as_deref() == session
    }) else {
        // Already acked (the prompt ran before this report) or superseded.
        return json!({ "name": name });
    };
    let failed = argv.iter().any(|a| a == "--failed");
    let record = if failed {
        None
    } else {
        inflight.confirmed_at = Some(crate::shared::time::now_epoch_i64());
        Some(json!(inflight).to_string())
    };
    if let Err(e) = db.kv_set(&ext_inflight_key(name), record.as_deref()) {
        return json!({ "error": format!("update in-flight batch: {e}") });
    }
    if failed {
        log::log_info(
            "hooks",
            "copilot.ext_send_failed",
            &format!(
                "instance={name} batch={}; redelivering",
                inflight.last_event_id
            ),
        );
    }
    json!({ "name": name })
}

/// How long a batch handed to the extension, but not yet reported accepted,
/// holds off redelivery. The extension reports within its hcom call timeout;
/// past this it is taken to have died before sending.
const EXT_INFLIGHT_LEASE_SECS: i64 = 60;

/// How long an accepted batch stays with its queued prompt after the
/// extension last confirmed it queued. The extension re-checks at least every
/// 30s (its safety poll), so a lapse means it is gone and cannot vouch for the
/// prompt any more.
const EXT_QUEUED_CONFIRM_SECS: i64 = 90;

fn ext_inflight_key(name: &str) -> String {
    format!("copilot_ext_inflight:{name}")
}

/// A batch the extension was given, acked by the UserPromptSubmit hook when
/// Copilot actually submits that prompt. Kept in the DB, not the extension, so
/// the ack survives the extension restarting, and only a prompt Copilot ran
/// counts as delivered.
#[derive(serde::Serialize, serde::Deserialize)]
struct ExtInflight {
    last_event_id: i64,
    status_context: String,
    msg_ts: String,
    text: String,
    sent_at: i64,
    /// The Copilot session the prompt was sent to; a queued prompt does not
    /// outlive a session switch (`/clear`).
    session_id: Option<String>,
    /// When the extension last reported the prompt accepted or still queued
    /// (`copilot-ext-sent`); `None` until the send is reported.
    confirmed_at: Option<i64>,
}

impl ExtInflight {
    fn load(db: &HcomDb, name: &str) -> Option<Self> {
        let raw = db.kv_get(&ext_inflight_key(name)).ok().flatten()?;
        serde_json::from_str(&raw).ok()
    }

    /// The batch, if still owed to a prompt the extension sent: ahead of the
    /// cursor (not delivered some other way), and either still being sent or
    /// confirmed queued in the caller's session (`session`: the polling
    /// extension's, else the instance's; a queued prompt does not survive
    /// `/clear`).
    fn live(db: &HcomDb, instance: &InstanceRow, session: Option<&str>) -> Option<Self> {
        let now = crate::shared::time::now_epoch_i64();
        Self::load(db, &instance.name).filter(|inflight| {
            let session = session.or(instance.session_id.as_deref());
            let same_session = match (inflight.session_id.as_deref(), session) {
                (Some(owner), Some(caller)) => owner == caller,
                _ => true,
            };
            inflight.last_event_id > instance.last_event_id
                && same_session
                && match inflight.confirmed_at {
                    Some(confirmed) => now - confirmed < EXT_QUEUED_CONFIRM_SECS,
                    None => now - inflight.sent_at < EXT_INFLIGHT_LEASE_SECS,
                }
        })
    }
}

/// UserPromptSubmit for the extension's delivery prompt: commit its batch.
/// Matched on the exact text, so a user prompt that happens to start with
/// `<hcom>` is not mistaken for it.
fn ack_ext_delivery(db: &HcomDb, name: &str, prompt: &str) -> bool {
    let Some(inflight) = ExtInflight::load(db, name) else {
        return false;
    };
    if inflight.text.trim() != prompt {
        return false;
    }
    let _ = db.kv_set(&ext_inflight_key(name), None);
    // A failed cursor write is logged and leaves the batch unread, so it is
    // delivered again rather than lost.
    common::commit_delivery_ack(
        db,
        &DeliveryAck {
            instance_name: name.to_string(),
            created_at: None,
            last_event_id: inflight.last_event_id,
            status_context: inflight.status_context,
            msg_ts: inflight.msg_ts,
            mark_announced: false,
            // The persisted extension batch predates an exact incarnation
            // receipt token; preserve its existing acknowledgement contract.
            receipt: None,
        },
    );
    log::log_info(
        "hooks",
        "copilot.ext_acked",
        &format!("instance={name} acked_to={}", inflight.last_event_id),
    );
    true
}

/// Argv entry point for the Copilot extension (`copilot-ext-*`), separate from
/// the stdin-JSON hooks Copilot itself runs.
pub fn dispatch_copilot_ext(hook_name: &str, argv: &[String]) -> (i32, String) {
    let db = match HcomDb::open() {
        Ok(db) => db,
        Err(err) => return (0, json!({ "error": format!("db open: {err}") }).to_string()),
    };
    let ctx = HcomContext::from_os();
    if !common::hook_gate_check(&ctx) {
        return (0, "{}".to_string());
    }
    let argv = match argv.split_first() {
        Some((first, rest)) if first == hook_name => rest,
        _ => argv,
    };
    let output = common::dispatch_with_panic_guard(
        "copilot",
        hook_name,
        json!({ "error": "internal panic" }),
        || match hook_name {
            "copilot-ext-poll" => handle_ext_poll(&db, &ctx, argv),
            "copilot-ext-sent" => handle_ext_sent(&db, &ctx, argv),
            _ => json!({ "error": format!("unknown Copilot extension command: {hook_name}") }),
        },
    );
    (0, output.to_string())
}

fn hook_type_for_command(hook_name: &str) -> &'static str {
    COPILOT_HOOK_COMMANDS
        .iter()
        .find(|(_, command, _, _)| *command == hook_name)
        .map(|(event, _, _, _)| *event)
        .unwrap_or("Unknown")
}

pub fn dispatch_copilot_hook_native(hook_name: &str) -> i32 {
    let raw: Value = match serde_json::from_reader(std::io::stdin().lock()) {
        Ok(value) => value,
        Err(err) => {
            log::log_warn(
                "hooks",
                "copilot.parse_error",
                &format!("hook={hook_name} err={err}"),
            );
            return 0;
        }
    };
    let db = match HcomDb::open() {
        Ok(db) => db,
        Err(err) => {
            log::log_warn(
                "hooks",
                "copilot.db_error",
                &format!("hook={hook_name} err={err}"),
            );
            return 0;
        }
    };
    let ctx = HcomContext::from_os();
    if !common::hook_gate_check(&ctx) {
        return 0;
    }
    let payload = HookPayload::from_copilot_native(hook_type_for_command(hook_name), raw);
    let (output, mut delivery_ack) =
        common::dispatch_with_panic_guard("copilot", hook_name, (json!({}), None), || {
            match hook_name {
                "copilot-sessionstart" => (handle_sessionstart(&db, &ctx, &payload), None),
                "copilot-userpromptsubmit" => (handle_userpromptsubmit(&db, &ctx, &payload), None),
                "copilot-pretooluse" => (handle_pretooluse(&db, &ctx, &payload), None),
                "copilot-permissionrequest" => {
                    (handle_permissionrequest(&db, &ctx, &payload), None)
                }
                "copilot-posttooluse" | "copilot-posttoolusefailure" => {
                    handle_posttooluse(&db, &ctx, &payload)
                }
                "copilot-agentstop" => handle_agentstop(&db, &ctx, &payload),
                // The subagent's own Stop (its session) already ran; the agent
                // continues, so there is nothing to change or deliver here.
                "copilot-subagentstop" => (json!({ "decision": "allow" }), None),
                "copilot-notification" => (handle_notification(&db, &ctx, &payload), None),
                "copilot-erroroccurred" => (handle_erroroccurred(&db, &ctx, &payload), None),
                "copilot-subagentstart" => (handle_subagentstart(&db, &ctx, &payload), None),
                "copilot-sessionend" => (handle_sessionend(&db, &ctx, &payload), None),
                _ => (json!({}), None),
            }
        });
    if let Some(ack) = &mut delivery_ack {
        common::attach_delivery_native_ids(ack, &payload);
    }
    let mut stdout = std::io::stdout().lock();
    if serde_json::to_writer(&mut stdout, &output).is_ok()
        && stdout.flush().is_ok()
        && let Some(ack) = delivery_ack.as_ref()
    {
        common::commit_delivery_ack_output(&db, ack, output.to_string().as_bytes());
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::test_helpers::EnvGuard;
    use serial_test::serial;

    fn copilot_test_env() -> (tempfile::TempDir, PathBuf, EnvGuard) {
        let guard = EnvGuard::new();
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let home = dir.path().join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&home).unwrap();
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("HCOM_DIR", workspace.join(".hcom"));
            std::env::remove_var("COPILOT_HOME");
        }
        (dir, workspace, guard)
    }

    /// Active instance `memo` bound to session `sid-fg` / process `proc-1`,
    /// with one unread message from luna.
    fn stop_test_db() -> (tempfile::TempDir, HcomDb, HcomContext) {
        crate::config::Config::init();
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_raw(&dir.path().join("test.db")).unwrap();
        db.init_db().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, status, status_context, status_time, created_at, last_event_id, session_id)
                 VALUES ('memo', 'copilot', 'active', 'tool:bash', 0, 0, 0, 'sid-fg')",
                [],
            )
            .unwrap();
        db.set_process_binding("proc-1", "sid-fg", "memo").unwrap();
        let data = json!({"from": "luna", "text": "hello", "scope": "broadcast"}).to_string();
        db.conn()
            .execute(
                "INSERT INTO events (type, timestamp, instance, data)
                 VALUES ('message', '2026-01-01T00:00:01Z', 'luna', ?1)",
                [data],
            )
            .unwrap();
        let env = [("HCOM_PROCESS_ID".to_string(), "proc-1".to_string())]
            .into_iter()
            .collect();
        let ctx = HcomContext::from_env(&env, dir.path().to_path_buf());
        (dir, db, ctx)
    }

    #[test]
    fn agentstop_with_pending_keeps_instance_active() {
        let (_dir, db, ctx) = stop_test_db();
        let stop = |session: &str| {
            handle_agentstop(
                &db,
                &ctx,
                &HookPayload::from_copilot_native("Stop", json!({"sessionId": session})),
            )
        };

        // A subagent's session stopping: no delivery, agent stays active.
        let (out, ack) = stop("sid-subagent");
        assert_eq!(out["decision"], "allow");
        assert!(ack.is_none());
        assert!(
            !db.is_idle("memo"),
            "a subagent stopping does not idle the agent"
        );
        assert_eq!(
            db.get_instance_full("memo")
                .unwrap()
                .unwrap()
                .session_id
                .as_deref(),
            Some("sid-fg"),
            "a subagent's hooks do not rebind the instance"
        );

        let (out, ack) = stop("sid-fg");
        assert_eq!(out["decision"], "block");
        assert!(ack.is_some());
        assert!(
            !db.is_idle("memo"),
            "listening would invite a second delivery"
        );

        db.conn()
            .execute("UPDATE instances SET last_event_id = 999", [])
            .unwrap();
        let (out, _) = stop("sid-fg");
        assert_eq!(out["decision"], "allow");
        assert!(db.is_idle("memo"));
    }

    #[test]
    #[serial]
    fn sessionstart_inside_first_turn_keeps_it_active() {
        let (_env_dir, _workspace, _guard) = copilot_test_env();
        let (_dir, db, ctx) = stop_test_db();
        db.set_status("memo", ST_ACTIVE, "prompt").unwrap();
        handle_sessionstart(
            &db,
            &ctx,
            &HookPayload::from_copilot_native("SessionStart", json!({"sessionId": "sid-fg"})),
        );
        assert_eq!(
            db.get_instance_full("memo").unwrap().unwrap().status,
            ST_ACTIVE
        );
    }

    #[test]
    fn sessionend_from_clear_keeps_pty_instance_and_frees_session() {
        let (_dir, db, ctx) = stop_test_db();
        db.conn()
            .execute("UPDATE instances SET tcp_mode = 1", [])
            .unwrap();
        let raw = json!({"sessionId": "sid-fg", "reason": "user_exit"});
        handle_sessionend(
            &db,
            &ctx,
            &HookPayload::from_copilot_native("SessionEnd", raw),
        );
        let inst = db.get_instance_full("memo").unwrap().unwrap();
        assert_eq!(inst.session_id, None);
        assert_eq!(inst.status, ST_LISTENING);
        assert!(db.get_session_binding("sid-fg").unwrap().is_none());
        assert_eq!(
            inst.name_announced, 0,
            "new session needs the bootstrap again"
        );
    }

    #[test]
    fn sessionend_only_stops_the_instances_own_session() {
        crate::config::Config::init();
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_raw(&dir.path().join("test.db")).unwrap();
        db.init_db().unwrap();
        // A resumed instance: launched with the resumed session id and bound
        // to the new Copilot process.
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, status, status_context, status_time, created_at, session_id)
                 VALUES ('memo', 'copilot', 'listening', 'ready', 0, 0, 'resumed-sid')",
                [],
            )
            .unwrap();
        db.set_process_binding("proc-new", "resumed-sid", "memo")
            .unwrap();
        let env = [("HCOM_PROCESS_ID".to_string(), "proc-new".to_string())]
            .into_iter()
            .collect();
        let ctx = HcomContext::from_env(&env, dir.path().to_path_buf());
        let end = |sid: &str| {
            let raw = json!({"sessionId": sid, "reason": "user_exit"});
            handle_sessionend(
                &db,
                &ctx,
                &HookPayload::from_copilot_native("SessionEnd", raw),
            )
        };

        end("startup-sid");
        let inst = db.get_instance_full("memo").unwrap().unwrap();
        assert_eq!(inst.session_id.as_deref(), Some("resumed-sid"));
        assert_eq!(inst.status, "listening");

        end("resumed-sid");
        assert!(db.get_instance_full("memo").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn per_run_plugin_contains_hooks_and_permissions() {
        let (_dir, workspace, _guard) = copilot_test_env();
        let mut ctx = LaunchCtx::ambient(crate::tool::Tool::Copilot, true);
        ctx.cwd = workspace;
        ctx.args = vec!["--model".into(), "gpt-5".into(), "--".into(), "hi".into()];
        let injection = prepare_per_run(&ctx).unwrap();
        let plugin_dir = PathBuf::from(&injection.args[3]);
        assert_eq!(&injection.args[..3], ["--model", "gpt-5", "--plugin-dir"]);
        assert_eq!(&injection.args[4..], ["--", "hi"]);
        assert!(plugin_dir.join("plugin.json").is_file());
        let root: Value =
            serde_json::from_slice(&std::fs::read(plugin_dir.join("hooks.json")).unwrap()).unwrap();
        assert!(
            root["hooks"]["SessionStart"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hook| hook["command"] == build_copilot_hook_command("copilot-sessionstart"))
        );
        assert!(
            root["hooks"]["PermissionRequest"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hook| hook["command"]
                    == build_copilot_hook_command("copilot-permissionrequest"))
        );
        assert!(
            root["hooks"]["ErrorOccurred"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hook| hook["command"] == build_copilot_hook_command("copilot-erroroccurred"))
        );
    }

    #[test]
    #[serial]
    fn per_run_extension_only_with_native_delivery() {
        let (dir, workspace, _guard) = copilot_test_env();
        let copilot_home = dir.path().join("copilot-home");
        let launch = |delivery: &str| {
            let mut ctx = LaunchCtx::ambient(crate::tool::Tool::Copilot, false);
            ctx.cwd = workspace.clone();
            ctx.env
                .insert("HCOM_COPILOT_DELIVERY".into(), delivery.into());
            ctx.env.insert(
                "COPILOT_HOME".into(),
                copilot_home.to_string_lossy().into_owned(),
            );
            let injection = prepare_per_run(&ctx).unwrap();
            let plugin_dir = PathBuf::from(&injection.args[1]);
            let manifest: Value =
                serde_json::from_slice(&std::fs::read(plugin_dir.join("plugin.json")).unwrap())
                    .unwrap();
            (injection, plugin_dir, manifest)
        };

        let (injection, plugin_dir, manifest) = launch("inject");
        assert!(manifest.get("extensions").is_none());
        assert!(!plugin_dir.join(EXTENSION_PATH).exists());
        assert!(injection.env.is_empty());
        assert!(!copilot_home.join("settings.json").exists());

        let (injection, plugin_dir, manifest) = launch("native");
        assert_eq!(manifest["extensions"], "extensions");
        assert_eq!(
            injection.env,
            [(EXTENSION_DELIVERY_ENV.to_string(), "1".to_string())]
        );
        let source = std::fs::read_to_string(plugin_dir.join(EXTENSION_PATH)).unwrap();
        let prefix = serde_json::to_string(&crate::runtime_env::get_hcom_prefix()).unwrap();
        assert!(source.contains(&format!("const HCOM = {prefix};")));
        let settings: Value =
            serde_json::from_slice(&std::fs::read(copilot_home.join("settings.json")).unwrap())
                .unwrap();
        assert_eq!(settings["enabledFeatureFlags"]["EXTENSIONS"], true);

        // Extensions explicitly turned off in Copilot, or unreadable settings:
        // the launch fails instead of overriding the choice or degrading.
        let fails_with = |settings: &str| {
            std::fs::write(copilot_home.join("settings.json"), settings).unwrap();
            let mut ctx = LaunchCtx::ambient(crate::tool::Tool::Copilot, false);
            ctx.cwd = workspace.clone();
            ctx.env
                .insert("HCOM_COPILOT_DELIVERY".into(), "native".into());
            ctx.env.insert(
                "COPILOT_HOME".into(),
                copilot_home.to_string_lossy().into_owned(),
            );
            assert!(prepare_per_run(&ctx).is_err());
            assert_eq!(
                std::fs::read_to_string(copilot_home.join("settings.json")).unwrap(),
                settings
            );
        };
        fails_with(r#"{"experimental": true, "extensions": {"mode": "disabled"}}"#);
        fails_with("{ not json");
    }

    fn ext_test_db() -> (tempfile::TempDir, HcomDb, HcomContext, i64) {
        let (dir, db, ctx) = stop_test_db();
        let message_id = db
            .conn()
            .query_row("SELECT MAX(id) FROM events", [], |row| row.get(0))
            .unwrap();
        (dir, db, ctx, message_id)
    }

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    fn submit(db: &HcomDb, ctx: &HcomContext, prompt: &str) -> InstanceRow {
        handle_userpromptsubmit(
            db,
            ctx,
            &HookPayload::from_copilot_native(
                "UserPromptSubmit",
                json!({"sessionId": "sid-fg", "prompt": prompt}),
            ),
        );
        db.get_instance_full("memo").unwrap().unwrap()
    }

    #[test]
    fn ext_poll_delivers_while_idle_and_prompt_submit_acks() {
        let (_dir, db, ctx, message_id) = ext_test_db();

        // Mid-turn: register the wake port, leave delivery to the hooks.
        let busy = handle_ext_poll(&db, &ctx, &argv(&["--notify-port", "4555"]));
        assert_eq!(busy, json!({ "name": "memo" }));
        assert_eq!(db.get_notify_endpoint_port("memo", "plugin"), Some(4555));

        db.set_status("memo", ST_LISTENING, "").unwrap();
        let idle = handle_ext_poll(&db, &ctx, &argv(&["--session-id", "sid-fg"]));
        let text = idle["text"].as_str().unwrap().to_string();
        assert!(text.contains("hello"));
        assert!(db.has_pending("memo"), "poll alone must not ack");

        // Handed out, prompt not run yet (or the extension restarted): no resend.
        let again = handle_ext_poll(&db, &ctx, &argv(&["--session-id", "sid-fg"]));
        assert_eq!(again["inflight"], message_id);
        assert!(again.get("text").is_none());

        // A user prompt that merely starts with <hcom> is not the delivery.
        let inst = submit(&db, &ctx, "<hcom> my own note");
        assert_eq!(inst.status_context, "prompt");
        assert!(db.has_pending("memo"));

        // Copilot runs the delivery prompt: acked.
        db.set_status("memo", ST_LISTENING, "").unwrap();
        let inst = submit(&db, &ctx, &format!("{text}\n"));
        assert!(!db.has_pending("memo"));
        assert_eq!(inst.status, ST_ACTIVE);
        assert_eq!(inst.status_context, "deliver:luna");
        assert!(ExtInflight::load(&db, "memo").is_none());
    }

    #[test]
    fn ext_inflight_batch_is_redelivered_after_lease() {
        let (_dir, db, ctx, _) = ext_test_db();
        db.set_status("memo", ST_LISTENING, "").unwrap();
        let first = handle_ext_poll(&db, &ctx, &argv(&[]));
        let mut inflight = ExtInflight::load(&db, "memo").unwrap();
        inflight.sent_at -= EXT_INFLIGHT_LEASE_SECS;
        db.kv_set(
            &ext_inflight_key("memo"),
            Some(&json!(inflight).to_string()),
        )
        .unwrap();
        let second = handle_ext_poll(&db, &ctx, &argv(&[]));
        assert_eq!(second["text"], first["text"]);
    }

    /// The extension's prompt queued behind a user turn that won the race:
    /// hooks leave its batch alone, however long that turn runs, and the
    /// queued prompt acks it when it finally runs.
    #[test]
    fn hooks_defer_to_batch_queued_by_extension() {
        let (_dir, db, ctx, message_id) = ext_test_db();
        db.set_status("memo", ST_LISTENING, "").unwrap();
        let poll = handle_ext_poll(&db, &ctx, &argv(&["--session-id", "sid-fg"]));
        let text = poll["text"].as_str().unwrap().to_string();
        assert_eq!(poll["event_id"], message_id);
        let event_id = message_id.to_string();
        handle_ext_sent(
            &db,
            &ctx,
            &argv(&["--event-id", &event_id, "--session-id", "sid-fg"]),
        );

        // The user's prompt runs first, for longer than the send lease.
        submit(&db, &ctx, "my own task");
        let mut inflight = ExtInflight::load(&db, "memo").unwrap();
        assert!(inflight.confirmed_at.is_some());
        inflight.sent_at -= EXT_INFLIGHT_LEASE_SECS * 2;
        db.kv_set(
            &ext_inflight_key("memo"),
            Some(&json!(inflight).to_string()),
        )
        .unwrap();
        let tool_use = HookPayload::from_copilot_native(
            "PostToolUse",
            json!({"sessionId": "sid-fg", "toolName": "bash"}),
        );
        let stop = HookPayload::from_copilot_native("Stop", json!({"sessionId": "sid-fg"}));
        assert_eq!(handle_posttooluse(&db, &ctx, &tool_use), (json!({}), None));
        let (out, ack) = handle_agentstop(&db, &ctx, &stop);
        assert_eq!((out, ack.is_none()), (json!({ "decision": "allow" }), true));
        assert!(db.has_pending("memo"));

        // Then the queued delivery prompt runs.
        submit(&db, &ctx, &text);
        assert!(!db.has_pending("memo"));
        assert!(ExtInflight::load(&db, "memo").is_none());
    }

    #[test]
    fn ext_send_outcome_releases_batch() {
        let (_dir, db, ctx, message_id) = ext_test_db();
        let event_id = message_id.to_string();
        let tool_use = HookPayload::from_copilot_native(
            "PostToolUse",
            json!({"sessionId": "sid-fg", "toolName": "bash"}),
        );
        let hooks_deliver = || handle_posttooluse(&db, &ctx, &tool_use).1.is_some();

        // Failed send: the batch is free again at once.
        db.set_status("memo", ST_LISTENING, "").unwrap();
        handle_ext_poll(&db, &ctx, &argv(&["--session-id", "sid-fg"]));
        assert!(!hooks_deliver());
        handle_ext_sent(
            &db,
            &ctx,
            &argv(&[
                "--event-id",
                &event_id,
                "--session-id",
                "sid-fg",
                "--failed",
            ]),
        );
        assert!(ExtInflight::load(&db, "memo").is_none());
        assert!(hooks_deliver());

        // Accepted, then the session switched (`/clear`): the queued prompt
        // went with the old session.
        handle_ext_poll(&db, &ctx, &argv(&["--session-id", "sid-fg"]));
        handle_ext_sent(
            &db,
            &ctx,
            &argv(&["--event-id", &event_id, "--session-id", "sid-fg"]),
        );
        assert!(!hooks_deliver());
        db.conn()
            .execute(
                "UPDATE instances SET session_id = 'sid-new' WHERE name = 'memo'",
                [],
            )
            .unwrap();
        let tool_use = HookPayload::from_copilot_native(
            "PostToolUse",
            json!({"sessionId": "sid-new", "toolName": "bash"}),
        );
        assert!(handle_posttooluse(&db, &ctx, &tool_use).1.is_some());

        // The new session's extension claims the batch; a late report from the
        // old session's extension leaves that claim alone.
        handle_ext_poll(&db, &ctx, &argv(&["--session-id", "sid-new"]));
        let stale = argv(&[
            "--event-id",
            &event_id,
            "--session-id",
            "sid-fg",
            "--failed",
        ]);
        handle_ext_sent(&db, &ctx, &stale);
        let claim = ExtInflight::load(&db, "memo").unwrap();
        assert_eq!(claim.session_id.as_deref(), Some("sid-new"));
        assert!(claim.confirmed_at.is_none());
    }

    #[test]
    fn ext_poll_ignores_other_sessions_and_trusts_copilot_idle() {
        let (_dir, db, ctx, _) = ext_test_db();

        let other = handle_ext_poll(
            &db,
            &ctx,
            &argv(&["--notify-port", "4556", "--session-id", "sid-bg", "--idle"]),
        );
        assert_eq!(other["skipped"], "session_mismatch");
        assert_eq!(db.get_notify_endpoint_port("memo", "plugin"), None);
        assert!(!db.is_idle("memo"));

        // Status stuck active (no Stop hook), Copilot says idle: deliver.
        let idle = handle_ext_poll(&db, &ctx, &argv(&["--session-id", "sid-fg", "--idle"]));
        assert!(db.is_idle("memo"));
        assert!(idle["text"].as_str().unwrap().contains("hello"));
    }

    #[test]
    #[serial]
    fn per_run_cleanup_preserves_unrelated_legacy_hooks() {
        let (_dir, workspace, _guard) = copilot_test_env();
        let hooks_path = workspace.join(".copilot/hooks/hcom.json");
        std::fs::create_dir_all(hooks_path.parent().unwrap()).unwrap();
        let mut root = json!({
            "version": 1,
            "hooks": {
                "SessionStart": [{ "type": "command", "command": "./custom-start.sh" }]
            }
        });
        merge_hcom_hooks(&mut root, true);
        std::fs::write(&hooks_path, serde_json::to_vec_pretty(&root).unwrap()).unwrap();
        let mut ctx = LaunchCtx::ambient(crate::tool::Tool::Copilot, false);
        ctx.cwd = workspace;
        cleanup_legacy_per_run(&ctx).unwrap();
        let root: Value = serde_json::from_slice(&std::fs::read(&hooks_path).unwrap()).unwrap();
        assert_eq!(
            root["hooks"]["SessionStart"],
            json!([{ "type": "command", "command": "./custom-start.sh" }])
        );
        assert!(root["hooks"].get("PermissionRequest").is_none());
    }

    #[test]
    #[serial]
    fn per_run_cleanup_deletes_hcom_only_legacy_file() {
        let (_dir, workspace, _guard) = copilot_test_env();
        let hooks_path = workspace.join(".copilot/hooks/hcom.json");
        std::fs::create_dir_all(hooks_path.parent().unwrap()).unwrap();
        let mut root = json!({ "version": 1 });
        merge_hcom_hooks(&mut root, true);
        std::fs::write(&hooks_path, serde_json::to_vec_pretty(&root).unwrap()).unwrap();
        let mut ctx = LaunchCtx::ambient(crate::tool::Tool::Copilot, false);
        ctx.cwd = workspace;
        cleanup_legacy_per_run(&ctx).unwrap();
        assert!(!hooks_path.exists());
    }

    #[test]
    fn remove_leaves_file_without_hcom_entries_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hcom.json");
        let source = r#"{"version":1,"hooks":{}}"#;
        std::fs::write(&path, source).unwrap();
        remove_hooks_at(&path).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), source);
    }

    #[test]
    fn safe_hcom_command_detection() {
        assert!(command_looks_safe_hcom("hcom send @luna -- hi"));
        assert!(command_looks_safe_hcom("uvx hcom list --json"));
        assert!(!command_looks_safe_hcom("hcom kill luna"));
        assert!(!command_looks_safe_hcom("echo hcom send @luna"));
        assert!(command_looks_safe_hcom("hcom"));
        assert!(command_looks_safe_hcom(
            "hcom send @luna --name nova -- 'costs $5; fine (really)'"
        ));
        assert!(command_looks_safe_hcom(r#"hcom send @luna -- "a; b | c""#));
        for chained in [
            "hcom send @luna -- hi; rm -rf ~",
            "hcom send @luna -- hi && rm -rf ~",
            "hcom list | sh",
            "hcom send @luna -- $(cat ~/.ssh/id_rsa)",
            r#"hcom send @luna -- "$(whoami)""#,
            "hcom send @luna -- `id`",
            "hcom list > /tmp/x",
            "hcom list\nrm -rf ~",
            "hcom send @luna -- 'unterminated",
            "hcomx list",
        ] {
            assert!(!command_looks_safe_hcom(chained), "{chained}");
        }
    }
}
