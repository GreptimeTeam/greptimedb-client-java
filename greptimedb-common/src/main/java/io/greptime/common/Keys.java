/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.greptime.common;

/**
 * System properties option keys
 *
 * @author jiachun.fjc
 */
public final class Keys {
    public static final String DB_NAME = "GreptimeDB";
    public static final String VERSION_KEY = "client.version";
    public static final String OS_NAME = "os.name";
    public static final String USE_OS_SIGNAL = "greptimedb.use_os_signal";
    public static final String AVAILABLE_CPUS = "greptimedb.available_cpus";
    public static final String SIG_OUT_DIR = "greptimedb.signal.out_dir";
    public static final String REPORT_PERIOD = "greptimedb.reporter.period_minutes";
    public static final String GRPC_CONN_RESET_THRESHOLD = "greptimedb.grpc.conn.failures.reset_threshold";
    public static final String GRPC_POOL_CORE_WORKERS = "greptimedb.grpc.pool.core_workers";
    public static final String GRPC_POOL_MAXIMUM_WORKERS = "greptimedb.grpc.pool.maximum_works";
    public static final String RW_LOGGING = "greptimedb.read.write.rw_logging";

    private Keys() {}
}
