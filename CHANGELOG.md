## [develop] - Current development version

### Add

### Change

### Fix

### Refactor

### Remove


## [1.3.14]

### Add
* feat: Add requirements to `setup.py` by @sveneberth in https://github.com/viur-framework/viur-datastore/pull/54
* feat: Implement multi Get testcases by @sveneberth in https://github.com/viur-framework/viur-datastore/pull/52
* feat: Add Pipfile for testing and releasing by @sveneberth in https://github.com/viur-framework/viur-datastore/pull/56

### Change
* Ensure exclude_from_indexes is a set by @ArneGudermann in https://github.com/viur-framework/viur-datastore/pull/44
* Replace assert by  is_viur_datastore_request_ok in `transport.pyx` by @ArneGudermann in https://github.com/viur-framework/viur-datastore/pull/37

### Fix
* fix: Remove duplicate Keys in _fixKind by @ArneGudermann in https://github.com/viur-framework/viur-datastore/pull/47
* fix: Add case if subKey is not None by @sveneberth in https://github.com/viur-framework/viur-datastore/pull/50
* fix: Refuse `Get` of incomplete keys by @sveneberth in https://github.com/viur-framework/viur-datastore/pull/53
* fix: Remove indent from changes from #44 by @sveneberth in https://github.com/viur-framework/viur-datastore/pull/55

### Refactor
* refactor: Add .editorconfig and reformat. by @ArneGudermann in https://github.com/viur-framework/viur-datastore/pull/45
* refactor: Fix some type hints by @sveneberth in https://github.com/viur-framework/viur-datastore/pull/49


## [1.3.13]

### Add

- feat: LocalMemcache (#39)

  * Add LocalMemcache
  * Implement LocalMemcache
  * Change Docs
  * Apply suggestions from code review

  Co-authored-by: Sven Eberth <se@mausbrand.de>

### Change

- chore: decrease minimal required python version to 3.10 (#41)

  Was increased in https://github.com/viur-framework/viur-datastore/commit/33e1f49d5b66af2f0cec17f4e60e069ba2b4eaee
  but this is incompatible for the viur-core

### Fix

- Fix Typo (#38)

## [1.3.12]

### Add

- basic support for memcache service

### Change

### Fix

- correct handling of digit-only keys 

### Refactor

### Remove

## [1.3.11]

### Fix

thx to @ArneGudermann and @skoegl for the contribution

- Fix RunInTransaction Fix (#32)

## [1.3.10]

### Fix

Thx to @ArneGudermann for the contribution

- Add AbortedError in RunInTransaction (#31)

## [1.3.9]

Thx to @XeoN-GHMB for the contribution

- feat: If no more results but query is not finished print a warning (#29)

### Change

- print a warning when zig zag merge algo does not provide a result on end of query run

## [1.3.8]

Thx to @phorward for the contribution

- Improve keyHelper function (#28)
- Improve Query.get_orders() (#27)

### Add

- Call recursively with decoded key based on a string to implement target kind checking once
  - `adjust_kind`-parameter to optionally allow to adjust invalid kind to the target kind; This is useful when rewriting keys is explicitly wanted.

### Change

- Raise NotImplementedError on unsupported key type

### Fix

- Fix ValueError raise to f-string
- Fix invalid str raise...

### Refactor

- cleaned up code

### Remove

## [1.3.7]

### Add

### Change

### Fix

- fix to_legacy_urlsafe (#26)

### Refactor

## [1.3.6]

### Add

- add count aggregation query (#23)

### Change

### Fix

- fulltextsearch _entryMatchesQuery need the actual filter and we need to test on QueryDefinition instead of dict (#24)

### Refactor

- Quarantee exclude_from_indexes is a set (#22)

## [1.3.5]

### Change

- Enable all exceptions/errors for verbose pprint in log (#20)

## [1.3.4]

### Added

- Provide Key.__str__() function again (#17)

### Changed

- Specified 3.10 as min python version in setup

### Fixed

- Fixed multi-querys (#15)

## [1.3.3]

### Added

- Provide Key.__str__() function again (#17)

### Changed

- Specified 3.9 as min python version in setup

### Fixed

- Fixed multi-querys (#15)

### Removed

## [1.3.1]

### Added

- Allow setting the endCursor from outside (#8)

### Changed

- Include 100 in limit range in fetch() (#10)

### Fixed

### Removed

## [1.3.0]

### Added

- Using exponential backoff for retrying commit
- Added complete ViurException hierarchy using the status error field and a mapping to our destinct errors.
- silencing out logging output from requests and urllib3
- introducing a config var 'verbose_error_codes' for selecting which error status should trigger a verbose message output on stdout/stderr 

### Changed

- The exception 'Collision' was renamed to 'CollisionError'. This change must be also be applied in viur-core.

### Fixed

### Removed

## [1.2.2]

### Added

.gitignore file

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




[develop]: https://github.com/viur-framework/viur-datastore/compare/1.3.14...master
[1.3.14]: https://github.com/viur-framework/viur-datastore/compare/v1.3.13...v1.3.14
[1.3.13]: https://github.com/viur-framework/viur-datastore/compare/v1.3.12...v1.3.13
[1.3.12]: https://github.com/viur-framework/viur-datastore/compare/v1.3.11...v1.3.12
[1.3.11]: https://github.com/viur-framework/viur-datastore/compare/v1.3.10...v1.3.11
[1.3.10]: https://github.com/viur-framework/viur-datastore/compare/v1.3.9...v1.3.10
[1.3.9]: https://github.com/viur-framework/viur-datastore/compare/v1.3.8...v1.3.9
[1.3.8]: https://github.com/viur-framework/viur-datastore/compare/v1.3.7...v1.3.8
[1.3.7]: https://github.com/viur-framework/viur-datastore/compare/v1.3.6...v1.3.7
[1.3.6]: https://github.com/viur-framework/viur-datastore/compare/v1.3.5...v1.3.6
[1.3.5]: https://github.com/viur-framework/viur-datastore/compare/v1.3.4...v1.3.5
[1.3.4]: https://github.com/viur-framework/viur-datastore/compare/v1.3.3...v1.3.4
[1.3.3]: https://github.com/viur-framework/viur-datastore/compare/v1.3.2...v1.3.3
[1.3.2]: https://github.com/viur-framework/viur-datastore/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/viur-framework/viur-datastore/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/viur-framework/viur-datastore/compare/v1.2.2...v1.3.0
[1.2.2]: https://github.com/viur-framework/viur-datastore/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/viur-framework/viur-datastore/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/viur-framework/viur-datastore/compare/v1.1...v1.2.0
