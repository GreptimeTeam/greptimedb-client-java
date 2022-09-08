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

import io.greptime.common.Endpoint;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import io.greptime.v1.GreptimeDB;
import io.greptime.v1.codec.Insert;

/**
 *
 * @author jiachun.fjc
 */
public final class InsertHelper {

    public static GreptimeDB.BatchRequest toWriteRequest(WriteRows rows) {
        Insert.InsertBatch batch = Insert.InsertBatch.newBuilder() //
            .addAllColumns(rows.getColumns()) //
            .setRowCount(rows.getRowCount()) //
            .build();

        Common.ExprHeader header = Common.ExprHeader.newBuilder() //
            .setVersion(0) // TODO version
            .build();

        Database.InsertExpr insert = Database.InsertExpr.newBuilder() //
            .setTableName(rows.tableName()) //
            .addValues(batch.toByteString()) //
            .build();

        Database.ObjectExpr obj = Database.ObjectExpr.newBuilder() //
            .setHeader(header) //
            .setInsert(insert) //
            .build();

        Database.DatabaseRequest databaseReq = Database.DatabaseRequest.newBuilder() //
            .setName("") // TODO db name
            .addExprs(obj) //
            .build();

        return GreptimeDB.BatchRequest.newBuilder() //
            .addDatabases(databaseReq) //
            .build();
    }

    public static Result<WriteOk, Err> toWriteResult(GreptimeDB.BatchResponse res, //
                                                     Endpoint endpoint, //
                                                     WriteRows rows) {
        return null;
    }

    private InsertHelper() {
    }
}
