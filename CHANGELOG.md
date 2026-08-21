<!--
This changelog should always be read on `main` branch. Its contents on other branches
does not necessarily reflect the changes.
-->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.1.0] - 2026-08-21

### Added
- Add AI observability fields from bfe-access-pb v0.2.0: `ai_provider`, `ai_retry_count`, `ai_cost_value`, `ai_cost_currency`, `ai_route_rule_hits`, `ai_cluster_key_names`, and `ai_auth_hit_quota_plans`.
- Add `HttpAddr` config option to bind the web monitor server to a specific listen address.
- Add integration test suite with a mock Kafka broker, covering the end-to-end access log flow (scenario LR01 basic flow).
- Add configuration documentation (`doc/configuration/`) and output field reference (`doc/fields/`).

### Changed
- Upgrade `bfe-access-pb` from v0.1.0 to v0.2.0.
- Rename AI observability fields to align with bfe-access-pb v0.2.0: `ai_apikey` → `ai_apikey_id`, `ai_mapped_model` → `ai_target_model`, `ai_prompt_tokens` → `ai_input_tokens`.
- Upgrade `bfe` dependency to the latest v1.8.5 branch version.
- Migrate `github.com/baidu/go-lib` dependency to `github.com/bfenetworks/go-lib`.

## [v1.0.0] - 2026-07-20

### Added

- Core log reader with protobuf-encoded BFE access log parsing and tailing support.
- Module framework (`reader_module`) for extensible log processing pipelines.
- Config loading system (`reader_conf`) with support for basic and access-pb config types.
- Built-in Kafka output module (`mod_kafka`) for forwarding parsed access logs to Kafka.

[Unreleased]: https://github.com/bfenetworks/log-reader/compare/v1.0.0...HEAD
[v1.0.0]: https://github.com/bfenetworks/log-reader/releases/tag/v1.0.0
