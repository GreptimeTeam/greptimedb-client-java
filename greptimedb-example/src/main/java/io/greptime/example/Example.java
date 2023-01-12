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
package io.greptime.example;

import io.greptime.GreptimeDB;
import io.greptime.Util;
import io.greptime.models.*;
import io.greptime.options.GreptimeOptions;
import io.greptime.rpc.RpcOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * @author jiachun.fjc
 */
public class Example {

    private static final Logger LOG = LoggerFactory.getLogger(Example.class);

    public static void main(String[] args) throws Exception {
        Util.resetRwLogging();

        /*
           At the time I wrote this, GreptimeDB did not yet support automatic `create table`
           for the gRPC protocol, so we needed to do it manually. For more details, please
           refer to [Table Management](https://docs.greptime.com/user-guide/table-management).

           ```SQL
            CREATE TABLE monitor (
                host STRING,
                ts BIGINT,
                cpu DOUBLE DEFAULT 0,
                memory DOUBLE NULL,
                TIME INDEX (ts),
                PRIMARY KEY(host)) ENGINE=mito WITH(regions=1);
            ```
         */
        Executor asyncPool = ForkJoinPool.commonPool();
        GreptimeOptions opts = GreptimeOptions.newBuilder("127.0.0.1:4001") //
            .writeMaxRetries(1) //
            .readMaxRetries(2) //
            .asyncPool(asyncPool, asyncPool) //
            .rpcOptions(RpcOptions.newDefault()) //
            .routeTableRefreshPeriodSeconds(-1) //
            .build();

        GreptimeDB greptimeDB = new GreptimeDB();

        if (!greptimeDB.init(opts)) {
            throw new RuntimeException("Fail to start GreptimeDB client");
        }

        Result<WriteOk, Err> writeResult = runInsert(greptimeDB);

        LOG.info("Write result: {}", writeResult);

        if (!writeResult.isOk()) {
            writeResult.getErr().getError().printStackTrace();
            return;
        }

        Result<QueryOk, Err> queryResult = runQuery(greptimeDB);

        LOG.info("Query result: {}", queryResult);

        if (!queryResult.isOk()) {
            queryResult.getErr().getError().printStackTrace();
            return;
        }

        SelectRows rows = queryResult.getOk().getRows();

        LOG.info("Selected data:");

        rows.forEachRemaining(row -> LOG.info("Row: {}", row.values()));
    }

    private static Result<WriteOk, Err> runInsert(GreptimeDB greptimeDB) throws Exception {
        WriteRows rows = WriteRows
            .newBuilder(TableName.with("public", "monitor"))
            .semanticTypes(SemanticType.Tag, SemanticType.Timestamp, SemanticType.Field, SemanticType.Field)
            .dataTypes(ColumnDataType.String, ColumnDataType.TimestampMillisecond, ColumnDataType.Float64,
                ColumnDataType.Float64).columnNames("host", "ts", "cpu", "memory").build();

        rows.insert("127.0.0.1", System.currentTimeMillis(), 0.1, null) //
            .insert("127.0.0.2", System.currentTimeMillis(), 0.3, 0.5) //
            .finish();

        return greptimeDB.write(rows).get();
    }

    private static Result<QueryOk, Err> runQuery(GreptimeDB greptimeDB) throws Exception {
        QueryRequest request = QueryRequest.newBuilder() //
            .exprType(SelectExprType.Sql) //
            .ql("SELECT * FROM monitor;") //
            .build();

        return greptimeDB.query(request).get();
    }
}
