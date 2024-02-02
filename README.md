# GreptimeDB Java Client

[![build](https://github.com/GreptimeTeam/greptimedb-client-java/actions/workflows/build.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-client-java/actions/workflows/build.yml)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![Maven Central](https://img.shields.io/maven-central/v/io.greptime/greptimedb-client.svg?label=maven%20central)](https://central.sonatype.com/search?q=io.greptime)

A Java Client for GreptimeDB, which is compatible with GreptimeDB protocol and lightweight.

> [!WARNING]  
> `greptimedb-client-java` has been discontinued and is deprecated. The code in this repository is no longer maintained. Please use our new ingester [greptimedb-ingester-java](https://github.com/GreptimeTeam/greptimedb-ingester-java)

## Features

- SPI-based extensible network transport layer; provides the default implementation by using the
  gRPC framework
- Non-blocking, purely asynchronous API, easy to use
- Automatically collects various performance metrics by default. Users can then configure them and
  write to local files
- Users can take in-memory snapshots of critical objects, configure them, and write to local files.
  This is helpful when troubleshooting complex issues

## [User Guide](https://docs.greptime.com/reference/sdk/java)

## Javadoc
- [greptimedb-protocol](https://javadoc.io/doc/io.greptime/greptimedb-protocol/latest/index.html)
- [greptimedb-rpc](https://javadoc.io/doc/io.greptime/greptimedb-rpc/latest/index.html)
- [greptimedb-grpc](https://javadoc.io/doc/io.greptime/greptimedb-grpc/latest/index.html)
- [greptimedb-common](https://javadoc.io/doc/io.greptime/greptimedb-common/latest/index.html)
