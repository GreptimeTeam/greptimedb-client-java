# GreptimeDB Java Client

[![build](https://github.com/GreptimeTeam/greptimedb-client-java/actions/workflows/build.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-client-java/actions/workflows/build.yml)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![Maven Central](https://img.shields.io/maven-central/v/io.greptime/greptimedb-client.svg?label=maven%20central)](https://central.sonatype.com/search?q=io.greptime)

A Java Client for GreptimeDB, which is compatible with GreptimeDB protocol and lightweight.

## Features

- SPI-based extensible network transport layer; provides the default implementation by using the
  gRPC framework
- Non-blocking, purely asynchronous API, easy to use
- Automatically collects various performance metrics by default. Users can then configure them and
  write to local files
- Users can take in-memory snapshots of critical objects, configure them, and write to local files.
  This is helpful when troubleshooting complex issues

## [User Guide](https://docs.greptime.com/user-guide/java-sdk/java-sdk)
