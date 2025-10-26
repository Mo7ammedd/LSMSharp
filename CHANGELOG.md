# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-10-26

### Added
- **Range Scan Feature**: Implemented async iterator-based range scanning with `RangeAsync()` method
  - Efficient range queries across memtables and SSTables
  - Automatic handling of tombstones and version conflicts
  - Sorted results by key
- **XML Documentation**: Comprehensive XML documentation for all public APIs
  - IntelliSense support in IDEs
  - Auto-generated documentation file
- **Database Statistics API**: New `GetDatabaseStats()` method for monitoring
  - Active and flushing memtable sizes
  - Flush operation status
- **Example Applications**: Added range scan demonstration example
- **API Documentation**: Comprehensive API.md with usage examples and best practices
- **CI/CD Pipeline**: GitHub Actions workflow for automated builds and tests
- **NuGet Package Support**: Package metadata and configuration for publishing
- **Contributing Guide**: CONTRIBUTING.md with development guidelines

### Changed
- Reorganized project structure with Examples directory
- Enhanced error messages and input validation
- Improved .gitignore to exclude example databases

### Fixed
- Build errors from multiple entry points
- Test namespace references in main Program.cs

### Documentation
- Added API.md with complete API reference
- Created CONTRIBUTING.md with contribution guidelines
- Updated project metadata for NuGet packaging

## [0.1.0] - Initial Implementation

### Features
- Core LSM-Tree implementation with leveled compaction
- Write-ahead logging (WAL) for durability
- Concurrent skip list for in-memory operations
- SSTable format with block-based storage
- Bloom filters for efficient key lookups
- Block-level compression (GZip, LZ4)
- Block caching for improved read performance
- CRUD operations (Set, Get, Delete)
- Manual flush and compaction triggers
- Comprehensive test suite (functional, performance, stress tests)
- Bloom filter benchmarks

[1.0.0]: https://github.com/Mo7ammedd/LSMSharp/releases/tag/v1.0.0
