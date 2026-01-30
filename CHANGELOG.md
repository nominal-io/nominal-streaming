# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.11](https://github.com/nominal-io/nominal-streaming/compare/v0.7.10...v0.7.11) - 2026-01-30

### Added

- make request dispatcher print the worker number upon startup ([#200](https://github.com/nominal-io/nominal-streaming/pull/200))

## [0.7.10](https://github.com/nominal-io/nominal-streaming/compare/v0.7.9...v0.7.10) - 2026-01-26

### Added

- support new streaming types in avro file ([#193](https://github.com/nominal-io/nominal-streaming/pull/193))

### Other

- add nix dev shell ([#195](https://github.com/nominal-io/nominal-streaming/pull/195))

## [0.7.9](https://github.com/nominal-io/nominal-streaming/compare/v0.7.8...v0.7.9) - 2026-01-21

### Added

- add StructPoints ([#188](https://github.com/nominal-io/nominal-streaming/pull/188))

## [0.7.8](https://github.com/nominal-io/nominal-streaming/compare/v0.7.7...v0.7.8) - 2026-01-18

### Other

- update Cargo.toml dependencies

## [0.7.7](https://github.com/nominal-io/nominal-streaming/compare/v0.7.6...v0.7.7) - 2026-01-18

### Other

- update Cargo.toml dependencies

## [0.7.6](https://github.com/nominal-io/nominal-streaming/compare/v0.7.5...v0.7.6) - 2026-01-09

### Added

- support init from shared conjure clients ([#178](https://github.com/nominal-io/nominal-streaming/pull/178))
- add support for uint64 ([#177](https://github.com/nominal-io/nominal-streaming/pull/177))

## [0.7.5](https://github.com/nominal-io/nominal-streaming/compare/v0.7.4...v0.7.5) - 2026-01-07

### Added

- expose IntoTimestamp in prelude ([#173](https://github.com/nominal-io/nominal-streaming/pull/173))

### Fixed

- update point count while MutexGuard is held ([#140](https://github.com/nominal-io/nominal-streaming/pull/140))

## [0.7.4](https://github.com/nominal-io/nominal-streaming/compare/v0.7.3...v0.7.4) - 2026-01-05

### Other

- update Cargo.toml dependencies

## [0.7.3](https://github.com/nominal-io/nominal-streaming/compare/v0.7.2...v0.7.3) - 2025-12-19

### Other

- *(deps)* bump nominal-api to 0.1046.0 ([#151](https://github.com/nominal-io/nominal-streaming/pull/151))

## [0.7.2](https://github.com/nominal-io/nominal-streaming/compare/v0.7.1...v0.7.2) - 2025-12-17

### Fixed

- load unflushed points count once per loop while dropping ([#133](https://github.com/nominal-io/nominal-streaming/pull/133))

### Other

- enable tracing for all tests ([#132](https://github.com/nominal-io/nominal-streaming/pull/132))
- prevent doctests from generating artifact ([#131](https://github.com/nominal-io/nominal-streaming/pull/131))

## [0.7.1](https://github.com/nominal-io/nominal-streaming/compare/v0.7.0...v0.7.1) - 2025-12-12

### Other

- bump nominal-api version ([#137](https://github.com/nominal-io/nominal-streaming/pull/137))

## [0.7.0](https://github.com/nominal-io/nominal-streaming/compare/v0.6.0...v0.7.0) - 2025-12-08

### Added

- [**breaking**] remove staging clients ([#128](https://github.com/nominal-io/nominal-streaming/pull/128))
- use platform verifier for TLS ([#123](https://github.com/nominal-io/nominal-streaming/pull/123))

### Other

- move README into lib.rs ([#99](https://github.com/nominal-io/nominal-streaming/pull/99))

## [0.6.0](https://github.com/nominal-io/nominal-streaming/compare/v0.5.8...v0.6.0) - 2025-10-29

### Added

- extend listener trait ([#88](https://github.com/nominal-io/nominal-streaming/pull/88))

## [0.5.8](https://github.com/nominal-io/nominal-streaming/compare/v0.5.7...v0.5.8) - 2025-10-23

### Other

- update Cargo.toml dependencies

## [0.5.7](https://github.com/nominal-io/nominal-streaming/compare/v0.5.6...v0.5.7) - 2025-10-23

### Other

- update repository link in cargo.toml files ([#65](https://github.com/nominal-io/nominal-streaming/pull/65))

## [0.5.6](https://github.com/nominal-io/nominal-streaming/compare/v0.5.5...v0.5.6) - 2025-10-22

### Added

- no user facing changes

## [0.5.5](https://github.com/nominal-io/nominal-streaming/compare/nominal-streaming-v0.5.4...nominal-streaming-v0.5.5) - 2025-10-22

### Fixed

- return readme documentation to crate, update readme in python package ([#55](https://github.com/nominal-io/nominal-streaming/pull/55))

## [0.5.4](https://github.com/nominal-io/nominal-streaming/compare/nominal-streaming-v0.5.3...nominal-streaming-v0.5.4) - 2025-10-22

### Other

- update Cargo.toml dependencies

## [0.5.3](https://github.com/nominal-io/nominal-streaming/compare/nominal-streaming-v0.5.2...nominal-streaming-v0.5.3) - 2025-10-21

### Added

- add python bindings for existing streaming pipelines ([#28](https://github.com/nominal-io/nominal-streaming/pull/28))

## [0.5.2](https://github.com/nominal-io/nominal-streaming/compare/v0.5.1...v0.5.2) - 2025-10-20

### Fixed

- re-expose AuthProvider ([#44](https://github.com/nominal-io/nominal-streaming/pull/44))
- keep appending to changelog at repo root ([#39](https://github.com/nominal-io/nominal-streaming/pull/39))

## [0.5.1](https://github.com/nominal-io/nominal-streaming/compare/v0.5.0...v0.5.1) - 2025-10-16

### Other

- update Cargo.toml dependencies

## [0.5.0](https://github.com/nominal-io/nominal-streaming/compare/v0.4.1...v0.5.0) - 2025-10-15

### Added

- create workspace with nominal-streaming package, allow customizing api base url ([#27](https://github.com/nominal-io/nominal-streaming/pull/27))

## [0.4.1](https://github.com/nominal-io/nominal-streaming/compare/v0.4.0...v0.4.1) - 2025-09-26

### Added

- add feature for enabling logging ([#18](https://github.com/nominal-io/nominal-streaming/pull/18))
- update nominal-api dependency past PROTOC build dependency ([#19](https://github.com/nominal-io/nominal-streaming/pull/19))

## [0.4.0](https://github.com/nominal-io/nominal-streaming/compare/v0.3.0...v0.4.0) - 2025-09-15

### Added

- improved API for point-by-point insertion ([#12](https://github.com/nominal-io/nominal-streaming/pull/12))
- enable retries for conjure client ([#14](https://github.com/nominal-io/nominal-streaming/pull/14))

### Fixed

- export datasource stream ([#16](https://github.com/nominal-io/nominal-streaming/pull/16))

### Other

- explain streaming in README ([#5](https://github.com/nominal-io/nominal-streaming/pull/5))

## [0.3.0](https://github.com/nominal-io/nominal-streaming/compare/v0.2.0...v0.3.0) - 2025-09-10

### Added

- rename datasource to dataset and add builder ([#13](https://github.com/nominal-io/nominal-streaming/pull/13))

### Fixed

- allow empty tags in `ChannelDescriptor` constructor ([#11](https://github.com/nominal-io/nominal-streaming/pull/11))
- fix company name in license ([#8](https://github.com/nominal-io/nominal-streaming/pull/8))

## [0.2.0](https://github.com/nominal-io/nominal-streaming/compare/v0.1.1...v0.2.0) - 2025-09-04

### Added

- add a `prelude` module ([#6](https://github.com/nominal-io/nominal-streaming/pull/6))

### Other

- reduce cloning ([#4](https://github.com/nominal-io/nominal-streaming/pull/4))

## [0.1.1](https://github.com/nominal-io/nominal-streaming/compare/v0.1.0...v0.1.1) - 2025-09-02

### Other

- add tests ([#2](https://github.com/nominal-io/nominal-streaming/pull/2))
