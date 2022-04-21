## [develop] - Current development version

### Added

### Changed

### Fixed

### Removed

## [1.2.2]

### Added

### Changed

### Fixed

- Now handles setting of fetch cursor to None more efficiently

### Removed

## [1.2.1]

### Added

### Changed

- Much more detailed README.md how to use and develop this library

### Fixed

### Removed

## [1.2.0]

### Added

### Changed

- Switched to 'semver' versioning scheme
- order specifiers are now a list of tuples with old style compatibility

### Fixed

### Removed

## [1.1]

### Added
- Re-added startDataAccessLog and endDataAccessLog functions

### Changed
- Replaced Beta classifier with Production/Stable


## [1.0]

### Added
- unit-test for in-filter on \_\_keys\_\_

### Fixed
- Fixed handling of incomplete/null keys


## [0.9]

### Changed
- Moved config variables from query.py to separate config file

### Fixed
- traceQueries now logs all queries

### Removed
- Implicit import from viur-core.config.SkeletonInstanceRef has now to be set from outside.


## [0.8]

### Fixed
- Missing imports & typos in query.py
- Compiler and build warnings

## [0.7]

### Added
- Export AllocateIDs by default (needed for the new import system in viur-core)

## [0.6]

### Fixed
- Passing an empty list to delete() raising an exception
- Deprecation warnings regarding simdjson::element:at 

## [0.5]

### Fixed
- Deserializing empty lists

## [0.4]

### Fixed
- exclude_from_index Flag on list properties
- Building on MacOS again
- Two conversion bugs in unittests

## [0.3]

### Added
- AllocateIDs function

### Fixed
- Building on MacOS
- Excluding properties from indexes
- acquireTransactionSuccessMarker

## [0.2]

### Added
- Allow Entities/List of Entities in delete()
- Changelog

### Fixed
- Catch key-comparisions with non-key types

## [0.1]
- Initial public release

