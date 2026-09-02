use std::collections::HashMap;

use serde::{Deserialize, Serialize};

const DEFAULT_GENERIC_THRESHOLD_BYTES: usize = 8192;
const DEFAULT_SENTINEL_HARD_CAP_BYTES: usize = 16 * 1024;
const DEFAULT_SENTINEL_MIN_BYTES: usize = 512;
const DEFAULT_MAX_FETCH_RESPONSE_BYTES: usize = 16 * 1024;
/// Below this many hidden bytes, eliding costs more than it saves: the
/// round trip through `tool_output_fetch` is worth roughly 100 KB of
/// context-equivalent (see `research/tool-output-truncation-tuning-
/// handoff-2026-09-02.md` §1), so hiding a few hundred bytes to save a
/// marker is a net loss. `MIN_ELIDE_BYTES` (`walker.rs`) already guards
/// the degenerate "marker costs more than the bytes it replaces" case;
/// this is the same idea with the round-trip cost priced in.
const DEFAULT_MIN_HIDDEN_BYTES: usize = 32 * 1024;

/// Knobs the walker actually reads. Per-string head/tail counts are
/// derived adaptively from `generic_threshold_bytes` at walk time;
/// per-array head/tail counts are derived from `sentinel_min_bytes` /
/// `sentinel_hard_cap_bytes` via a shrink loop.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ToolCachingConfig {
    /// Strings / containers larger than this trigger elision.
    #[serde(default = "default_generic_threshold_bytes")]
    pub generic_threshold_bytes: usize,
    /// Lower bound for the array-sentinel size-shrink loop budget.
    #[serde(default = "default_sentinel_min_bytes")]
    pub sentinel_min_bytes: usize,
    /// Upper bound for the array-sentinel size-shrink loop budget.
    #[serde(default = "default_sentinel_hard_cap_bytes")]
    pub sentinel_hard_cap_bytes: usize,
    /// Ceiling on a single `tool_output_fetch` response. Recovery
    /// templates the walker emits stay under this by construction; ad
    /// hoc fetches with a bigger `len` or no `query` get rejected with
    /// `FetchResponseTooLarge`.
    #[serde(default = "default_max_fetch_response_bytes")]
    pub max_fetch_response_bytes: usize,
    /// Below this many hidden bytes (`total_bytes - shown_bytes` at the
    /// primary path), skip eliding entirely and pass the whole value
    /// through — the round trip to recover it would cost more than the
    /// bytes it hides.
    #[serde(default = "default_min_hidden_bytes")]
    pub min_hidden_bytes: usize,
    /// Per-tool overrides, keyed by the prefixed tool name (e.g.
    /// `library_get_files`). Falls back to the fields above when a tool
    /// has no entry, or when an entry doesn't set a given field.
    #[serde(default)]
    pub per_tool: HashMap<String, ToolCachingOverride>,
}

/// Per-tool override for a subset of [`ToolCachingConfig`]'s fields.
/// Unset fields fall back to the global default.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ToolCachingOverride {
    #[serde(default)]
    pub generic_threshold_bytes: Option<usize>,
    #[serde(default)]
    pub min_hidden_bytes: Option<usize>,
}

impl Default for ToolCachingConfig {
    fn default() -> Self {
        Self {
            generic_threshold_bytes: DEFAULT_GENERIC_THRESHOLD_BYTES,
            sentinel_min_bytes: DEFAULT_SENTINEL_MIN_BYTES,
            sentinel_hard_cap_bytes: DEFAULT_SENTINEL_HARD_CAP_BYTES,
            max_fetch_response_bytes: DEFAULT_MAX_FETCH_RESPONSE_BYTES,
            min_hidden_bytes: DEFAULT_MIN_HIDDEN_BYTES,
            per_tool: HashMap::new(),
        }
    }
}

fn default_generic_threshold_bytes() -> usize {
    DEFAULT_GENERIC_THRESHOLD_BYTES
}
fn default_sentinel_hard_cap_bytes() -> usize {
    DEFAULT_SENTINEL_HARD_CAP_BYTES
}
fn default_sentinel_min_bytes() -> usize {
    DEFAULT_SENTINEL_MIN_BYTES
}
fn default_max_fetch_response_bytes() -> usize {
    DEFAULT_MAX_FETCH_RESPONSE_BYTES
}
fn default_min_hidden_bytes() -> usize {
    DEFAULT_MIN_HIDDEN_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_tool_override_deserializes_partial_fields() {
        let cfg: ToolCachingConfig = serde_json::from_value(serde_json::json!({
            "per_tool": {
                "library_get_files": { "generic_threshold_bytes": 65536 },
            }
        }))
        .unwrap();
        let over = cfg.per_tool.get("library_get_files").unwrap();
        assert_eq!(over.generic_threshold_bytes, Some(65536));
        assert_eq!(over.min_hidden_bytes, None);
        // Untouched globals keep their defaults.
        assert_eq!(cfg.generic_threshold_bytes, DEFAULT_GENERIC_THRESHOLD_BYTES);
        assert_eq!(cfg.min_hidden_bytes, DEFAULT_MIN_HIDDEN_BYTES);
    }

    #[test]
    fn absent_config_yields_defaults() {
        let cfg: ToolCachingConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(cfg.min_hidden_bytes, DEFAULT_MIN_HIDDEN_BYTES);
        assert!(cfg.per_tool.is_empty());
    }
}
