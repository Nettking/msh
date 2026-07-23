# Graph Report - C:\wsl\msh  (2026-07-23)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 2170 nodes · 5597 edges · 106 communities (97 shown, 9 thin omitted)
- Extraction: 96% EXTRACTED · 4% INFERRED · 0% AMBIGUOUS · INFERRED: 218 edges (avg confidence: 0.62)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- scan_artifacts
- intervention_strategy_runner.py
- export_jsonl.py
- pipeline.py
- SourceInventoryService
- data_filtering.py
- step2.py
- overview_service.py
- data_analysis.py
- ScriptOption
- test_control_dataset_ux.py
- playback_service.py
- vision2.py
- find_stops.py
- telemetry_cache.py
- operator_strategy_service.py
- webapp/app.py
- ai_routes.py
- data_sources.py
- state_inference.py
- TelemetryCache
- routes.py
- test_playback_service.py
- session_store.py
- create_app
- server_setup_service.py
- LiveTelemetryService
- test_connected_ai_provider.py
- log
- data_loading.py
- OslExportService
- OperatorStrategyService
- test_telemetry_cache_integration.py
- tool_wear_indicator.py
- AgentError
- msh-phone.sh
- Chunk
- live_service.py
- timeline_exports.py
- exploration
- playback
- OperatorSupportService
- standalone-recorder_v2.py
- repo_index.py
- operator_support_routes.py
- server_setup_routes.py
- control_service.py
- operator_page_cache.py
- prepare_playback_frame
- setup_msh.py
- webapp/playback.py
- SpeechWorker
- desktop-agent.py
- auto_connect.py
- process_folder
- rag.py
- RecommenderArtifactService
- test_ai_explainer_chat.py
- ControlPanelService
- standalone_recorder.py
- setup-phone.sh
- test_ai_explainer.py
- cached_cache_status
- OperatorConfirmationService
- request_structured_vision
- symbols.py
- FirstPartService
- MachineNotesService
- _session_is_linked_to_active_runtime
- workflow_session_index.py
- initialize_agent_runtime
- compact_text
- data_pr_day.py
- test_post_setup_handoff.py
- test_termux_phone_script.py
- ValueError
- get_workflow_session_index
- test_startup_saved_landing.py
- OperatorScope
- test_operator_strategy_page.py
- analysis_views.py
- test_guide_page.py
- test_pipeline_bootstrap.py
- default_cache_dir
- analyze_image
- ui.py
- test_static_css.py
- test_policies.py
- data_simulator.py
- _write_jsonl
- _serialize_playback_timestamp
- capture_workflow_screenshot
- ai/__init__.py
- common/__init__.py
- flask_app/__init__.py
- services/__init__.py
- observer_phoenix/__init__.py
- runner/__init__.py
- tool_wear_indicator/__init__.py

## God Nodes (most connected - your core abstractions)
1. `scan_artifacts()` - 50 edges
2. `ScanSnapshot` - 48 edges
3. `OperatorStrategyService` - 46 edges
4. `ArtifactCatalog` - 44 edges
5. `RuntimeOrchestrator` - 39 edges
6. `SourceInventoryService` - 36 edges
7. `TelemetryCache` - 34 edges
8. `_FakeCatalog` - 30 edges
9. `log()` - 29 edges
10. `_FakeRuntime` - 28 edges

## Surprising Connections (you probably didn't know these)
- `Form` --uses--> `OperatorStrategyService`  [INFERRED]
  scripts/seed_operator_support_demo.py → catalog/flask_app/services/operator_strategy_service.py
- `main()` --calls--> `OperatorStrategyService`  [EXTRACTED]
  scripts/seed_operator_support_demo.py → catalog/flask_app/services/operator_strategy_service.py
- `_interactive_settings()` --references--> `ServerSetupSettings`  [EXTRACTED]
  setup_msh.py → catalog/flask_app/services/server_setup_service.py
- `_run_compose()` --references--> `ServerSetupSettings`  [EXTRACTED]
  setup_msh.py → catalog/flask_app/services/server_setup_service.py
- `_settings_from_args()` --references--> `ServerSetupSettings`  [EXTRACTED]
  setup_msh.py → catalog/flask_app/services/server_setup_service.py

## Import Cycles
- None detected.

## Communities (106 total, 9 thin omitted)

### Community 0 - "scan_artifacts"
Cohesion: 0.05
Nodes (86): Process-local artifact catalog refresh notifications.  The runtime runner and, Register the process-local callback used to refresh the Flask catalog., Request a non-blocking artifact catalog refresh if one is registered., register_artifact_catalog_refresh(), request_artifact_catalog_refresh(), _artifact_category(), _artifact_visibility(), _build_artifact() (+78 more)

### Community 1 - "intervention_strategy_runner.py"
Cohesion: 0.07
Nodes (77): build_strategy_summary(), _coerce_yaml_scalar(), _dump_strategies_yaml(), _empty_candidates(), _event_rows(), intervention_strategy_config_signature(), load_label_config(), load_strategy_config() (+69 more)

### Community 2 - "export_jsonl.py"
Cohesion: 0.06
Nodes (65): format_utc(), load_state(), parse_utc(), Any, datetime, Path, RuntimeError, Small helpers for source-specific telemetry synchronization.  The helpers in t (+57 more)

### Community 3 - "pipeline.py"
Cohesion: 0.05
Nodes (52): main(), Summarize missing sequence numbers per day from compact derived telemetry metric, configured_scan_dirs(), basic_metrics_path(), build_basic_metrics_dataset(), iter_basic_metrics_rows(), datetime, Path (+44 more)

### Community 4 - "SourceInventoryService"
Cohesion: 0.05
Nodes (58): capture(), delete(), index(), mark_reusable(), review(), save(), _service(), _source_inventory() (+50 more)

### Community 5 - "data_filtering.py"
Cohesion: 0.06
Nodes (71): CaptureFixture, date_from_filename(), parse_iso_timestamp(), parse_timestamp_to_date(), date, datetime, Path, Utility functions for parsing timestamps and extracting dates.  This module pr (+63 more)

### Community 6 - "step2.py"
Cohesion: 0.07
Nodes (57): browser_runtime_choice_form_path(), capture_page_screenshot(), choose_runtime_start(), complete_first_time_setup(), enumerate_visible_windows(), establish_browser_runtime_session(), extract_json_object(), find_browser_window() (+49 more)

### Community 7 - "overview_service.py"
Cohesion: 0.10
Nodes (47): build_overview_snapshot(), _context_note(), _current_session_playback_status(), _headline_summary(), _latest_known_timestamp(), _machine_activity(), _overview_decision(), _playback_state_for_overview() (+39 more)

### Community 8 - "data_analysis.py"
Cohesion: 0.10
Nodes (37): add_date_column(), add_machine_id_column(), find_machine_column(), prepare_machine_telemetry_dataframe(), prepare_timestamp_column(), DataFrame, Series, Shared lightweight DataFrame preparation helpers for telemetry scripts.  This (+29 more)

### Community 9 - "ScriptOption"
Cohesion: 0.12
Nodes (30): Minimal backward-compatible runner utility shim.  New code should import from, discover_runnable_scripts(), Path, Script discovery and runner-visible script metadata., Discover runnable catalog scripts from catalog subdirectories., Description of one runnable catalog script., Return the repository root directory., Extract a short script description from the first line of a module docstring. (+22 more)

### Community 10 - "test_control_dataset_ux.py"
Cohesion: 0.17
Nodes (21): _CaptureControlService, _FakeCache, _FakeRuntime, _FakeScope, _FakeScopeService, _FakeSessionIndex, _make_app(), _metadata() (+13 more)

### Community 11 - "playback_service.py"
Cohesion: 0.13
Nodes (26): _active_runtime_namespace(), _artifact_index_key(), _best_artifact_for_machine_day(), _cached_playback_selection_index(), _entry_machine_days(), _namespace_from_auto_session_id(), playback_artifact_runtime_preference(), playback_selection_index() (+18 more)

### Community 12 - "vision2.py"
Cohesion: 0.12
Nodes (30): Exception, analyze_image(), build_error_section(), build_success_section(), call_ollama(), create_image_message(), encode_image(), find_images() (+22 more)

### Community 13 - "find_stops.py"
Cohesion: 0.11
Nodes (27): find_stop_rows(), group_stop_rows(), prepare_stop_numeric_columns(), DataFrame, Shared helpers for stop-row detection and stop-interval grouping.  These utili, Coerce available stop-related numeric columns to numeric dtype.      Returns a, Return stop-like rows after coercing stop numeric columns.      This helper fi, Group nearby stop rows into machine-specific stop intervals. (+19 more)

### Community 14 - "telemetry_cache.py"
Cohesion: 0.15
Nodes (21): CacheBuildResult, _coerce_telemetry_frame(), latest_cache_timestamp(), load_jsonl_records(), _manifest_sources_match(), _partition_value(), Any, DataFrame (+13 more)

### Community 15 - "operator_strategy_service.py"
Cohesion: 0.18
Nodes (17): _bool_from_form(), _coerce_bool(), _format_utc(), _infer_action_type(), _infer_review_status(), _normalize_record(), OperatorStrategyError, OperatorStrategyRecord (+9 more)

### Community 16 - "webapp/app.py"
Cohesion: 0.17
Nodes (29): _filter_machine_day(), _has_non_empty_values(), _load_data_from_path(), _load_data_from_upload(), _load_registry_frame(), _load_table_from_path(), _load_table_from_upload(), main() (+21 more)

### Community 17 - "ai_routes.py"
Cohesion: 0.14
Nodes (25): _bounded_context(), main(), parse_args(), Namespace, Command-line entry point for the read-only MSH AI explainer.  Usage:     pyth, chat(), OllamaError, RuntimeError (+17 more)

### Community 18 - "data_sources.py"
Cohesion: 0.12
Nodes (27): load_data_from_path(), load_data_from_upload(), load_registry_frame(), load_table_from_path(), load_table_from_upload(), parse_bootstrap_args(), DataFrame, Namespace (+19 more)

### Community 19 - "state_inference.py"
Cohesion: 0.15
Nodes (24): build_fired_rules(), extract_intervention_candidates(), group_boolean_events(), DataFrame, Event/window and interval helpers for telemetry state analysis., Select candidate intervention rows with a stable export schema., Group nearby event rows into broader event windows., Merge neighboring rows with identical state into intervals. (+16 more)

### Community 20 - "TelemetryCache"
Cohesion: 0.21
Nodes (24): cache_status(), discover_jsonl_files(), invalidate_cache_status(), Path, Discover raw telemetry JSONL files recursively under ``data_dir``.      Files, Rebuild the full Parquet cache from raw JSONL files.      The rebuild is atomi, Return whether the cache exists and matches the current JSONL source set., Clear cached cache-status entries after rebuilds or explicit refreshes. (+16 more)

### Community 21 - "routes.py"
Cohesion: 0.14
Nodes (25): choose_startup_mode(), control(), get_started(), _load_telemetry_cache_exploration_frame(), _machine_day_chart_payload(), _machine_day_csv_for_session(), _machine_day_detail_for_session(), _machine_day_detail_from_cache() (+17 more)

### Community 22 - "test_playback_service.py"
Cohesion: 0.16
Nodes (23): compute_playback_delay(), filter_playback_artifacts_for_runtime(), Hide workflow playback exports that do not belong to the active clean runtime., Compute a bounded client delay between telemetry samples.      Bad timestamps,, _CaptureLogger, Path, test_candidate_event_extract_is_not_classified_as_playback_when_timeline_exists(), test_compute_playback_delay_caps_large_gaps() (+15 more)

### Community 23 - "session_store.py"
Cohesion: 0.15
Nodes (24): _load_normalized_session_metadata(), Map operator scope controls to an existing or newly prepared session., _load_or_create_auto_session(), Return dates whose on-disk sessions satisfy the automatic contract.          R, filter_signature(), initialize_session_metadata(), list_sessions(), normalize_session_metadata() (+16 more)

### Community 24 - "create_app"
Cohesion: 0.17
Nodes (15): create_app(), Flask, FakeRuntimeManager, _patch_runtime(), _patch_setup(), test_get_started_is_a_focused_task_handoff(), test_knowledge_navigation_opens_a_choice_page(), test_main_navigation_pages_load() (+7 more)

### Community 25 - "server_setup_service.py"
Cohesion: 0.19
Nodes (21): compare_ollama_setup_models(), Any, Setup-time AI model comparison and recommendation helpers., Benchmark installed standard setup models and recommend a setup profile., _recommend(), _result_band(), _result_elapsed(), _row() (+13 more)

### Community 26 - "LiveTelemetryService"
Cohesion: 0.20
Nodes (17): get_live_telemetry_service(), LiveTelemetryService, _app_with_catalog(), _artifact(), _FakeCatalog, _FakeRuntime, Flask, Path (+9 more)

### Community 27 - "test_connected_ai_provider.py"
Cohesion: 0.18
Nodes (23): ai_settings_from_form(), default_settings(), migrate_legacy_phone_bootstrap(), normalize_ollama_base_url(), Path, RuntimeError, Validate and normalize an Ollama HTTP endpoint used for server-side calls., Apply only AI/provider form values to settings for save and pre-save tests. (+15 more)

### Community 28 - "log"
Cohesion: 0.15
Nodes (22): capture_cursor_screenshot(), click_github_control(), finalize_agent_runtime(), launch_or_focus_github_desktop(), log(), move_cursor_normalized(), move_cursor_to_neutral_position(), Capture the screen with an unmistakable physical-mouse marker.      A blue foc (+14 more)

### Community 29 - "data_loading.py"
Cohesion: 0.18
Nodes (19): iter_jsonl_files(), iter_jsonl_records(), iter_records_in_dir(), iter_records_with_parsed_timestamps(), load_jsonl_dataframe(), Any, Path, Shared helpers for loading JSONL records used by catalog scripts.  This module (+11 more)

### Community 30 - "OslExportService"
Cohesion: 0.21
Nodes (15): osl_export(), run_osl_export(), _clean(), _confidence(), _dt_keyword_lines(), _keyword_lines(), OslExportService, Any (+7 more)

### Community 31 - "OperatorStrategyService"
Cohesion: 0.16
Nodes (15): strategy_comparison(), OperatorStrategyService, Path, Any, _situation_key(), _strategy_summary(), StrategyComparisonService, test_capture_cannot_skip_review_or_treat_zero_as_true() (+7 more)

### Community 32 - "test_telemetry_cache_integration.py"
Cohesion: 0.23
Nodes (13): _app_with_catalog(), _artifact(), _FakeCatalog, _FakeRuntime, _FakeScope, _FakeScopeService, Flask, Path (+5 more)

### Community 33 - "tool_wear_indicator.py"
Cohesion: 0.20
Nodes (20): build_candidate_events(), build_signal_summary(), _default_output_path(), _first_present(), _load_signal_frame(), main(), parse_args(), Any (+12 more)

### Community 34 - "AgentError"
Cohesion: 0.13
Nodes (21): AgentError, ensure_github_desktop_maximized(), foreground_window_looks_like_github_desktop(), get_foreground_window_handle(), get_window_title(), is_window_maximized(), open_msh_repository_in_explorer(), Path (+13 more)

### Community 35 - "msh-phone.sh"
Cohesion: 0.22
Nodes (17): container_exists(), doctor(), http_ready(), login_supports_detach(), main(), proot_distro_supports_command(), remember_server_pid(), require_ready() (+9 more)

### Community 36 - "Chunk"
Cohesion: 0.19
Nodes (16): allowed_source_paths(), append_grounding_warning(), _is_plausible_repo_file_path(), Grounding guardrails for AI explainer answers., Return exact file paths that were supplied to the model as context., Return whether a path-like string should be treated as a repo file reference., Extract plausible repository file references from an answer., Return file references that were not in the retrieved context. (+8 more)

### Community 37 - "live_service.py"
Cohesion: 0.25
Nodes (17): CacheStatus, Freshness information for the Parquet telemetry cache., _CacheEntry, _data_dir_from_source_artifacts(), LiveSnapshot, _machine_from_path(), _normalized_live_state(), _path_machine_hints() (+9 more)

### Community 38 - "timeline_exports.py"
Cohesion: 0.20
Nodes (18): build_state_interval_export(), build_timeline_rows_export(), _ensure_timeline_columns(), export_timeline_for_machine_day(), export_timeline_rows(), infer_timeline_rows(), load_timeline_export(), load_timeline_export_with_schema_info() (+10 more)

### Community 39 - "exploration"
Cohesion: 0.21
Nodes (17): analyses(), exploration(), DataFrame, safe_load_artifact_frame(), _aggregate_time_frame(), category_columns(), category_counts(), histogram_data() (+9 more)

### Community 40 - "playback"
Cohesion: 0.13
Nodes (19): playback(), default_live_signal_columns(), _has_non_empty_values(), load_playback_frame(), playback_field_groups(), PlaybackValidation, Series, Validation result for playback source/file contract checks. (+11 more)

### Community 41 - "OperatorSupportService"
Cohesion: 0.12
Nodes (15): action_requires_confirmation(), ActionRisk, AlternativeAction, CauseHypothesis, EvidenceItem, _now_utc(), OperatorConfirmation, ProblemObservation (+7 more)

### Community 42 - "standalone-recorder_v2.py"
Cohesion: 0.19
Nodes (15): ensure_dir(), extract_mtconnect_values(), fetch_loop(), flush_buffer_to_disk(), flush_loop(), now_iso_utc(), Any, Poll MTConnect endpoints and persist new telemetry snapshots to JSONL.  The re (+7 more)

### Community 43 - "repo_index.py"
Cohesion: 0.31
Nodes (16): build_chunks(), _chunk_from_dict(), chunk_text(), _file_fingerprint(), iter_indexed_files(), load_cached_chunks(), load_or_build_chunks(), Any (+8 more)

### Community 44 - "operator_support_routes.py"
Cohesion: 0.20
Nodes (12): confirm_assist_action(), first_part(), learning(), machine_notes(), quality_outcomes(), save_quality_outcome(), _source_inventory(), _now_utc() (+4 more)

### Community 45 - "server_setup_routes.py"
Cohesion: 0.27
Nodes (17): browser_setup_gate(), compare_ai_models(), _compare_ai_models_from_request(), _legacy_shape_for_ai_page(), _next_path(), _pull_from_request(), Ensure one-command Docker startup lands in browser setup first.      This runs, _save_from_request() (+9 more)

### Community 46 - "control_service.py"
Cohesion: 0.25
Nodes (16): _analysis_outputs(), _manual_session_id(), _normalized_metadata_view(), _outputs_summary(), _playback_summary_from_metadata(), Any, date, _range_label() (+8 more)

### Community 47 - "operator_page_cache.py"
Cohesion: 0.17
Nodes (11): get_control_panel_service(), _CacheEntry, OperatorPageCache, Any, Path, Short-lived UI snapshot cache for operator-facing Flask pages.  Overview and c, Return cached/rebuilt control snapshot for the selected session., Cache expensive page snapshots until inputs or short TTLs change. (+3 more)

### Community 48 - "prepare_playback_frame"
Cohesion: 0.16
Nodes (18): interval_rows(), playback_context(), playback_day_counts_by_machine(), playback_days_by_machine(), playback_subset(), prepare_playback_frame(), DataFrame, Normalize a playback table to rows with timestamp, machine, state, and day. (+10 more)

### Community 49 - "setup_msh.py"
Cohesion: 0.23
Nodes (17): compose_profiles_for(), env_lines_for(), test_command_setup_can_create_edge_model_provider_node(), test_command_setup_can_select_connected_provider(), test_phone_bootstrap_can_leave_browser_setup_pending(), test_provider_node_cannot_disable_or_delegate_its_model(), test_provider_node_starts_only_provider_and_installs_model(), _interactive_settings() (+9 more)

### Community 50 - "webapp/playback.py"
Cohesion: 0.18
Nodes (17): filter_machine_day(), _has_non_empty_values(), playback_controls(), plot_signals(), plot_state_timeline(), DataFrame, Series, Timestamp (+9 more)

### Community 51 - "SpeechWorker"
Cohesion: 0.16
Nodes (10): RuntimeError, Synchronous offline Windows text-to-speech.      The worker prefers a more nat, Return the best matching installed voice name., Initialize Windows SAPI and select the best available voice., Initialize the pyttsx3 fallback and select a preferred voice., Initialize Windows SAPI, with pyttsx3 as a fallback., Make technical status text more pleasant to hear.          This keeps the fact, Speak one message immediately.          Speech is synchronous so the workflow (+2 more)

### Community 52 - "desktop-agent.py"
Cohesion: 0.19
Nodes (16): classify_msh_startup(), open_command_prompt_in_current_explorer_folder(), press_hotkey(), Outcome from one high-level workflow phase., Announce and perform a deterministic wait., Speak, log, and execute one keyboard shortcut., Speak, log, type text, and press Enter., Open Command Prompt from the verified Explorer address bar.      The address b (+8 more)

### Community 53 - "auto_connect.py"
Cohesion: 0.22
Nodes (15): locate_button_fast(), on_click(), on_key_press(), on_mouse_move(), on_scroll(), Desktop automation utility with user-activity pause protection.  This script r, Locate the center of a template image on the current screen.      Parameters, Perform one automation cycle.      The script first performs a small cursor mo (+7 more)

### Community 54 - "process_folder"
Cohesion: 0.21
Nodes (15): add_summary_metrics(), detect_override_changes_fast(), detect_state_episodes_fast(), preprocess(), process_folder(), DataFrame, Path, Extract intervention-state episodes and override-change events from MTConnect JS (+7 more)

### Community 55 - "rag.py"
Cohesion: 0.23
Nodes (13): _chunk_overlaps_symbol(), _literal_bonus(), _query_paths(), _query_routes(), Retrieval for the read-only AI explainer., Boost exact path, route, decorator, and function-name matches., Score a chunk by token overlap plus exact path/route boosts., score_chunk() (+5 more)

### Community 56 - "RecommenderArtifactService"
Cohesion: 0.20
Nodes (8): generate_recommender_artifacts(), recommender_artifacts(), Any, Path, RecommenderArtifactService, Form, dict, test_operator_strategy_lifecycle()

### Community 57 - "test_ai_explainer_chat.py"
Cohesion: 0.21
Nodes (12): _consume_list(), Conservative Markdown rendering for AI Explainer answers., Escape arbitrary text, then add a deliberately small inline subset., Render common answer structure without allowing model-supplied HTML., _render_inline(), render_safe_markdown(), _app(), Flask (+4 more)

### Community 58 - "ControlPanelService"
Cohesion: 0.21
Nodes (6): ControlPanelService, ControlRun, Validate a control action, reserve the single worker slot, and start it., Resolve /control requests into session-scoped background actions., _tail_snippet(), _utc_now_iso()

### Community 59 - "standalone_recorder.py"
Cohesion: 0.21
Nodes (13): extract_mtconnect_values(), fetch_loop(), flush_buffer_to_disk(), flush_loop(), Poll MTConnect endpoints and record new telemetry snapshots to daily JSONL files, Extract a flat dictionary of values from MTConnect XML.      Parameters     -, Poll all configured MTConnect sources and append new snapshots to the buffer., Flush the current in-memory buffer to the daily JSONL output file.      Behavi (+5 more)

### Community 60 - "setup-phone.sh"
Cohesion: 0.31
Nodes (13): container_exists(), container_runtime_ready(), ensure_termux_prerequisites(), fail(), http_ready(), main(), parse_args(), runtime_signature() (+5 more)

### Community 61 - "test_ai_explainer.py"
Cohesion: 0.42
Nodes (12): Return the most relevant chunks for a question., retrieve(), Path, test_iter_indexed_files_excludes_legacy_and_binary_like_outputs(), test_repo_root_from_finds_root(), test_retrieve_boosts_exact_repo_path(), test_retrieve_boosts_route_literals(), test_retrieve_prefers_matching_text_and_path() (+4 more)

### Community 62 - "cached_cache_status"
Cohesion: 0.21
Nodes (13): cached_cache_status(), Return cache status with a short TTL to protect request hot paths.      ``cach, _entry_machines(), load_cached_playback_frame_for_machine_day(), _playback_index_entry_for_path(), PlaybackArtifactIndexEntry, Path, Lightweight machine/day index data for one playback artifact. (+5 more)

### Community 63 - "OperatorConfirmationService"
Cohesion: 0.32
Nodes (6): assist(), _now_utc(), OperatorConfirmationService, Any, Path, _text()

### Community 64 - "request_structured_vision"
Cohesion: 0.24
Nodes (13): decode_json_object(), extract_model_content(), InvalidDecisionError, post_ollama_request(), Any, Decode one JSON object from a model response.      Structured-output models oc, Perform lightweight structural validation., Capture the current screen and request one small structured result.      Each (+5 more)

### Community 65 - "symbols.py"
Cohesion: 0.32
Nodes (11): AST, build_symbols(), _decorator_name(), extract_python_symbols(), _node_end_line(), Path, Repository symbol indexing for the AI explainer.  The symbol index is intentio, Extract functions, classes, and Flask routes from a Python file. (+3 more)

### Community 66 - "FirstPartService"
Cohesion: 0.32
Nodes (6): save_first_part(), FirstPartService, _now_utc(), Any, Path, _text()

### Community 67 - "MachineNotesService"
Cohesion: 0.32
Nodes (6): save_machine_note(), MachineNotesService, _now_utc(), Any, Path, _text()

### Community 68 - "_session_is_linked_to_active_runtime"
Cohesion: 0.50
Nodes (4): Mirror automatic workflow session-id namespace sanitization., Return true when session naming clearly links a session to the active run., _safe_runtime_namespace_for_session_id(), _session_is_linked_to_active_runtime()

### Community 69 - "workflow_session_index.py"
Cohesion: 0.21
Nodes (8): Path, Timestamp, TTL cache for listing workflow sessions from disk.  Session directories can gr, Cache freshness-sorted workflow session listings for Flask views., Return sessions sorted by best available freshness timestamp., _session_freshness_key(), SessionIndexResult, WorkflowSessionIndex

### Community 70 - "initialize_agent_runtime"
Cohesion: 0.17
Nodes (12): check_ollama_connection(), initialize_agent_runtime(), minimize_console_window(), OllamaError, Raised when communication with Ollama fails., Validate the small set of active workflow settings., Initialize shared resources once for the complete run., Make technical text concise enough for a spoken status message. (+4 more)

### Community 71 - "compact_text"
Cohesion: 0.20
Nodes (12): compact_text(), fetch_is_due(), GitHubState, inspect_github_state(), parse_fetch_age(), Small, focused description of the GitHub Desktop toolbar state., Normalize arbitrary text for logs and speech., Convert GitHub Desktop's short age text into the 12-hour policy category. (+4 more)

### Community 72 - "data_pr_day.py"
Cohesion: 0.24
Nodes (10): _build_machine_day_summary(), DataFrame, Path, Generate canonical machine/day CSV data from JSONL telemetry.  Behavior: - Re, Report malformed JSONL input lines during record loading.      The shared load, Report records whose timestamp cannot be parsed., _resolve_machine_day_output_csv(), _resolve_session_dir() (+2 more)

### Community 73 - "test_post_setup_handoff.py"
Cohesion: 0.31
Nodes (6): _app(), FakeRuntimeManager, _form(), test_editing_saved_setup_preserves_requested_destination(), test_first_setup_hands_off_to_get_started(), test_first_setup_keeps_get_started_after_session_choice()

### Community 74 - "test_termux_phone_script.py"
Cohesion: 0.44
Nodes (9): _phone_environment(), Path, _setup_environment(), test_changed_runtime_signature_triggers_rebuild_without_pkg_refresh(), test_existing_compatible_container_uses_fast_setup_without_pkg_or_build(), test_stop_does_not_claim_success_while_http_responds(), test_stop_finds_a_session_started_before_pid_tracking(), test_update_stops_before_pull_and_requests_automatic_restart() (+1 more)

### Community 75 - "ValueError"
Cohesion: 0.27
Nodes (10): load_all_data(), main(), preprocess(), Train baseline per-machine models for future-stop exploration.  This manual de, Train and evaluate one RandomForest stop-prediction model for a machine., Run the full per-machine stop-prediction pipeline., Load and combine all top-level JSONL files into one time-indexed DataFrame., Resample one machine's telemetry and construct model-ready features.      This (+2 more)

### Community 76 - "get_workflow_session_index"
Cohesion: 0.33
Nodes (10): _catalog(), control_action(), control_scope(), live(), overview(), refresh(), rescan(), run_script_control() (+2 more)

### Community 77 - "test_startup_saved_landing.py"
Cohesion: 0.36
Nodes (6): _configured_full_server_settings(), FakeRuntimeManager, _patch_runtime(), _patch_setup_context(), test_saved_startup_edit_mode_shows_wizard(), test_saved_startup_shows_runtime_landing_instead_of_wizard()

### Community 79 - "test_operator_strategy_page.py"
Cohesion: 0.36
Nodes (5): FakeRuntimeManager, _patch_runtime(), _patch_setup(), test_operator_strategy_capture_is_statement_first(), test_operator_strategy_review_explains_later_structuring()

### Community 80 - "analysis_views.py"
Cohesion: 0.28
Nodes (8): DataFrame, Analysis-centric Streamlit views for status, browser, and machine summaries., Render system-level indexing status and recent read errors., Render analysis listing and return the selected artifact record., Render machine-centric aggregate views from the selected analysis dataset., render_analyses_browser(), render_machine_view(), render_system_status()

### Community 81 - "test_guide_page.py"
Cohesion: 0.36
Nodes (4): FakeRuntimeManager, _patch_runtime(), _patch_setup(), test_guide_page_explains_knowledge_flow()

### Community 82 - "test_pipeline_bootstrap.py"
Cohesion: 0.36
Nodes (5): _option(), Path, test_bootstrap_analysis_uses_automatic_playback_ready_contract_in_contract_order(), test_reused_auto_session_metadata_is_updated_to_active_runtime_namespace(), test_runtime_state_snapshot_exposes_playback_filter_contract_keys()

### Community 83 - "default_cache_dir"
Cohesion: 0.32
Nodes (7): main(), parse_args(), Namespace, CLI entry point for rebuilding the Parquet telemetry cache., _cache_status_key(), default_cache_dir(), Return the conventional cache location under a data directory.

### Community 84 - "analyze_image"
Cohesion: 0.48
Nodes (6): analyze_image(), encode_image(), main(), Path, Les bildet og returner det Base64-kodet., Send ett bilde til Qwen3-VL gjennom Ollama.

### Community 85 - "ui.py"
Cohesion: 0.33
Nodes (5): print_numbered_menu(), prompt_menu_choice(), Runner CLI display and input helpers., Prompt the user for a numeric menu choice., Print a numbered menu to stdout.

### Community 86 - "test_static_css.py"
Cohesion: 0.80
Nodes (4): _rule_body(), _styles(), test_primary_buttons_override_section_button_surface(), test_secondary_and_hover_buttons_keep_explicit_readable_text_color()

### Community 88 - "data_simulator.py"
Cohesion: 0.50
Nodes (3): load_data(), Interactive Streamlit playback tool for JSONL telemetry data.  This app loads, Load and cache telemetry data from JSONL files.      The loader scans top-leve

### Community 89 - "_write_jsonl"
Cohesion: 0.67
Nodes (3): Path, test_load_prepared_frames_reads_nested_session_data(), _write_jsonl()

### Community 90 - "_serialize_playback_timestamp"
Cohesion: 0.67
Nodes (3): Series, _serialize_playback_timestamp(), test_serialize_playback_timestamp_preserves_millisecond_precision()

### Community 92 - "capture_workflow_screenshot"
Cohesion: 0.50
Nodes (4): capture_workflow_screenshot(), Convert a descriptive label into a safe evidence filename., Capture and save one plain screenshot., safe_filename()

## Knowledge Gaps
- **9 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `OperatorStrategyService` connect `OperatorStrategyService` to `SourceInventoryService`, `OperatorSupportService`, `operator_strategy_service.py`, `RecommenderArtifactService`, `OslExportService`?**
  _High betweenness centrality (0.040) - this node is a cross-community bridge._
- **Why does `TelemetryCache` connect `TelemetryCache` to `live_service.py`, `playback`, `playback_service.py`, `telemetry_cache.py`, `routes.py`, `LiveTelemetryService`, `cached_cache_status`?**
  _High betweenness centrality (0.036) - this node is a cross-community bridge._
- **Why does `ArtifactCatalog` connect `scan_artifacts` to `pipeline.py`, `live_service.py`, `overview_service.py`, `get_workflow_session_index`, `operator_page_cache.py`, `routes.py`, `create_app`, `LiveTelemetryService`?**
  _High betweenness centrality (0.030) - this node is a cross-community bridge._
- **Are the 15 inferred relationships involving `scan_artifacts()` (e.g. with `test_candidate_event_extract_is_not_classified_as_playback_when_timeline_exists()` and `test_overview_inventory_counts_workflow_timeline_export_as_playback()`) actually correct?**
  _`scan_artifacts()` has 15 INFERRED edges - model-reasoned connections that need verification._
- **Are the 18 inferred relationships involving `ScanSnapshot` (e.g. with `OverviewSnapshot` and `_FakeCatalog`) actually correct?**
  _`ScanSnapshot` has 18 INFERRED edges - model-reasoned connections that need verification._
- **Are the 7 inferred relationships involving `OperatorStrategyService` (e.g. with `OperatorSupportService` and `OslExportService`) actually correct?**
  _`OperatorStrategyService` has 7 INFERRED edges - model-reasoned connections that need verification._
- **Are the 15 inferred relationships involving `ArtifactCatalog` (e.g. with `_CacheEntry` and `LiveSnapshot`) actually correct?**
  _`ArtifactCatalog` has 15 INFERRED edges - model-reasoned connections that need verification._