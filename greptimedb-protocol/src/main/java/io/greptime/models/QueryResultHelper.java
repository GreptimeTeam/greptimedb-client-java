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

import com.google.protobuf.ByteStringHelper;
import com.google.protobuf.InvalidProtocolBufferException;
import io.greptime.Status;
import io.greptime.common.Endpoint;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import io.greptime.v1.GreptimeDB;
import io.greptime.v1.codec.Select;

/**
 *
 * @author jiachun.fjc
 */
public final class QueryResultHelper {

    /**
     * Converts the given {@link io.greptime.v1.GreptimeDB.BatchResponse} to {@link Result} that
     * upper-level readable.
     *
     * @param res        the query response
     * @param ql         query text, sash as sql
     * @param to         the server address
     * @param errHandler the error handler
     * @return a readable query result
     */
    public static Result<QueryOk, Err> from(GreptimeDB.BatchResponse res, String ql, Endpoint to, Runnable errHandler) {
        Database.DatabaseResponse db = res.getDatabases(0); // single write request
        Database.ObjectResult obj = db.getResults(0); // single write request
        Common.ResultHeader header = obj.getHeader();
        Database.SelectResult rawSelect = obj.getSelect();

        int statusCode = header.getCode();
        String errMsg = header.getErrMsg();

        if (!Status.isSuccess(statusCode)) {
            if (errHandler != null) {
                errHandler.run();
            }
            return Err.queryErr(statusCode, errMsg, to, ql).mapToResult();
        }

        Select.SelectResult select;
        try {
            byte[] rawData = ByteStringHelper.sealByteArray(rawSelect.getRawData());
            select = Select.SelectResult.parseFrom(rawData);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        return QueryOk.ok(ql, select.getRowCount(), SelectRows.from(select)).mapToResult();
    }

    private QueryResultHelper() {
    }
}
