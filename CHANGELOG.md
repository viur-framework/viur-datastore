## [develop] - Current development version

### Added
- Re-added startDataAccessLog and endDataAccessLog functions

### Changed

### Fixed

### Removed


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

