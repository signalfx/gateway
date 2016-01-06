# Change Log

All notable changes to this project will be documented in this file.
Placeholder changes in the oldest release exist only to document which
subsections are relevant.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [0.9.0] - 2015-01-05

### Removed

- Command line options were removed.  Users must now use config files to
  configure the proxy

### Changed

- Internally, the logging library logrus was replaced with golib/log.  Log
  output has changed as a result.
- Some metric names have changed.
- Status page now uses expvar at /debug/vars

## [0.8.0] - 2015-10-12

### Added

- Support delimiter based metric deconstructor usable for dimensionalizing of
  graphite metrics

## [0.7.0] - 2015-07-13

### Added

- Support notifications sent from collectd write_http and send to api as events

## [0.6.0] - 2015-06-17

### Added

- Parse dimensions from instance names of the format name[k=v]suffix

## [0.5.0] - 2015-05-21

### Added

- Added a changelog
- Carbon datapoint now support mtypedim options

## [0.4.0] - 2015-05-08

### Added

- Placeholder

### Deprecated

- Placeholder

### Removed

- Placeholder

### Changed

- Placeholder

### Fixed

- Placeholder

### Security

- Placeholder

[unreleased]: https://github.com/signalfx/metricproxy/compare/v0.9.0...HEAD
[0.4.0]: https://github.com/signalfx/metricproxy/compare/v0.0.1...v0.4.0
[0.5.0]: https://github.com/signalfx/metricproxy/compare/v0.4.0...v0.5.0
[0.6.0]: https://github.com/signalfx/metricproxy/compare/v0.5.0...v0.6.0
[0.7.0]: https://github.com/signalfx/metricproxy/compare/v0.6.0...v0.7.0
[0.8.0]: https://github.com/signalfx/metricproxy/compare/v0.7.0...v0.8.0
[0.9.0]: https://github.com/signalfx/metricproxy/compare/v0.8.0...v0.9.0
