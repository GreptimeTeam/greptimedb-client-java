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
package io.greptime.models;

import io.greptime.Status;
import io.greptime.common.Endpoint;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import io.greptime.v1.GreptimeDB;

/**
 *
 * @author jiachun.fjc
 */
public final class WriteResultHelper {

    /**
     * Converts the given {@link io.greptime.v1.GreptimeDB.BatchResponse} to {@link Result}
     * that upper-level readable.
     *
     * @param res  the wrote response
     * @param to   the server address wrote to
     * @param rows wrote data in this RPC
     * @return a readable write result
     */
    public static Result<WriteOk, Err> from(GreptimeDB.BatchResponse res, Endpoint to, WriteRows rows) {
        Database.DatabaseResponse db = res.getDatabases(0); // single write request
        Database.ObjectResult obj = db.getResults(0); // single write request
        Common.ResultHeader header = obj.getHeader();
        Common.MutateResult mutate = obj.getMutate();

        int statusCode = header.getCode();
        String errMsg = header.getErrMsg();
        int success = mutate.getSuccess();
        int failure = mutate.getFailure();

        if (Status.isSuccess(statusCode)) {
            return WriteOk.ok(success, failure, rows.tableName()).mapToResult();
        }

        return Err.writeErr(statusCode, errMsg, to, rows).mapToResult();
    }

    private WriteResultHelper() {
    }
}
