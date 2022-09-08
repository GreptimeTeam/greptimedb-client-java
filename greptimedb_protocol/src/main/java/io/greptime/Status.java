/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.greptime;

import java.util.HashMap;
import java.util.Map;

/**
 * Common status code for public API.
 *
 * @author jiachun.fjc
 */
public enum Status {
    // ====== Begin of common status code ==============
    // Success.
    Success(0),

    // Unknown error.
    Unknown(1000),
    // Unsupported operation.
    Unsupported(1001),
    // Unexpected error, maybe there is a BUG.
    Unexpected(1002),
    // Internal server error.
    Internal(1003),
    // Invalid arguments.
    InvalidArguments(1004),
    // ====== End of common status code ================

    // ====== Begin of SQL related status code =========
    // SQL Syntax error.
    InvalidSyntax(2000),
    // ====== End of SQL related status code ===========

    // ====== Begin of query related status code =======
    // Fail to create a plan for the query.
    PlanQuery(3000),
    // The query engine fail to execute query.
    EngineExecuteQuery(3001),
    // ====== End of query related status code =========

    // ====== Begin of catalog related status code =====
    // Table already exists.
    TableAlreadyExists(4000),
    //
    TableNotFound(4001),
    //
    TableColumnNotFound(4002),
    // ====== End of catalog related status code =======

    // ====== Begin of storage related status code =====
    // Storage is temporarily unable to handle the request
    StorageUnavailable(5000),
    // ====== End of storage related status code =======

    // ====== Begin of server related status code =====
    // Runtime resources exhausted, like creating threads failed.
    RuntimeResourcesExhausted(6000),
    // ====== End of server related status code =======
    ;

    private static final Map<Integer, Status> DICT = new HashMap<>();

    static {
        for (Status s : Status.values()) {
            DICT.put(s.getStatusCode(), s);
        }
    }

    private final int                         statusCode;
    private final boolean                     shouldRetry;

    Status(int statusCode) {
        this(statusCode, false);
    }

    Status(int statusCode, boolean shouldRetry) {
        this.statusCode = statusCode;
        this.shouldRetry = shouldRetry;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public boolean isShouldRetry() {
        return shouldRetry;
    }

    public static Status parse(int statusCode) {
        return DICT.get(statusCode);
    }
}
