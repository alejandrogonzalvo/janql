# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1](https://github.com/alejandrogonzalvo/janql/compare/v0.3.0...v0.3.1) - 2026-01-04

### Added

- add compaction policies
- implement manual compaction

## [0.3.0](https://github.com/alejandrogonzalvo/janql/compare/v0.2.3...v0.3.0) - 2025-12-29

### Added

- [**breaking**] add sstable implementation with WAL

## [0.2.3](https://github.com/alejandrogonzalvo/janql/compare/v0.2.2...v0.2.3) - 2025-12-29

### Added

- add startswith range query

### Other

- run benchmark release generation only on release-plz PRs
- *(bench)* add startswith bench

## [0.2.2](https://github.com/alejandrogonzalvo/janql/compare/v0.2.1...v0.2.2) - 2025-12-28

### Added

- implement binary encoding

### Other

- create artifact to compare with last version
- avoid benchmark action duplicated trigger
- use PAT for PR action trigger
- trigger benchmark-release on edited PRs
- *(fix)* benchmark-release triggers on release-plz MRs

## [0.2.1](https://github.com/alejandrogonzalvo/janql/compare/v0.2.0...v0.2.1) - 2025-12-28

### Other

- add 0.2.0 benchmark
- remove RUST_LOG debug

## [0.1.0](https://github.com/alejandrogonzalvo/janql/releases/tag/v0.1.0) - 2025-12-27

### Added

- implement append log version

### Other

- add benchmarks
- first commit
