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
package io.greptime.example;

import io.greptime.GreptimeDB;
import io.greptime.StreamWriter;
import io.greptime.models.ColumnDataType;
import io.greptime.models.Err;
import io.greptime.models.PromRangeQuery;
import io.greptime.models.QueryOk;
import io.greptime.models.QueryRequest;
import io.greptime.models.Result;
import io.greptime.models.SelectExprType;
import io.greptime.models.SelectRows;
import io.greptime.models.SemanticType;
import io.greptime.models.TableName;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.models.WriteRows;
import io.greptime.options.GreptimeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author jiachun.fjc
 */
public class QuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(QuickStart.class);

    public static void main(String[] args) throws Exception {
        String endpoint = "127.0.0.1:4001";
        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoint) //
                .writeMaxRetries(1) //
                .readMaxRetries(2) //
                .routeTableRefreshPeriodSeconds(-1) //
                .build();

        GreptimeDB greptimeDB = new GreptimeDB();

        if (!greptimeDB.init(opts)) {
            throw new RuntimeException("Fail to start GreptimeDB client");
        }

        long now = System.currentTimeMillis();
        LOG.info("now = {}", now);

        // normal inset
        runInsert(greptimeDB, now);

        // streaming insert
        runInsertWithStream(greptimeDB, now);

        runQuery(greptimeDB, now);

        runPromQueryRange(greptimeDB, now);
    }

    private static void runInsert(GreptimeDB greptimeDB, long now) throws Exception {
        TableSchema schema =
                TableSchema
                        .newBuilder(TableName.with("public", "monitor"))
                        .semanticTypes(SemanticType.Tag, SemanticType.Timestamp, SemanticType.Field,
                                SemanticType.Field, SemanticType.Field)
                        .dataTypes(ColumnDataType.String, ColumnDataType.TimestampMillisecond, ColumnDataType.Float64,
                                ColumnDataType.Float64, ColumnDataType.Decimal128) //
                        .columnNames("host", "ts", "cpu", "memory", "decimal_value") //
                        .build();

        WriteRows rows = WriteRows.newBuilder(schema).build();
        rows.insert("127.0.0.1", now, 0.1, null, new BigDecimal("128.111")) //
                .insert("127.0.0.2", now, 0.3, 0.5, new BigDecimal("1111111111111111111.2333333")) //
                .finish();

        // For performance reasons, the SDK is designed to be purely asynchronous.
        // The return value is a future object. If you want to immediately obtain
        // the result, you can call `future.get()`.
        CompletableFuture<Result<WriteOk, Err>> future = greptimeDB.write(rows);

        Result<WriteOk, Err> result = future.get();

        if (result.isOk()) {
            LOG.info("Write result: {}", result.getOk());
        } else {
            LOG.error("Failed to write: {}", result.getErr());
        }
    }

    private static void runInsertWithStream(GreptimeDB greptimeDB, long now) throws Exception {
        TableName tableName = TableName.with("public", "monitor");
        TableSchema
                .newBuilder(tableName)
                .semanticTypes(SemanticType.Tag, SemanticType.Timestamp, SemanticType.Field, SemanticType.Field,
                        SemanticType.Field)
                .dataTypes(ColumnDataType.String, ColumnDataType.TimestampMillisecond, ColumnDataType.Float64,
                        ColumnDataType.Float64, ColumnDataType.Decimal128) //
                .columnNames("host", "ts", "cpu", "memory", "decimal_value") //
                .buildAndCache(); // cache for reuse
        StreamWriter<WriteRows, WriteOk> streamWriter = greptimeDB.streamWriter();

        for (int i = 0; i < 100; i++) {
            WriteRows rows = WriteRows.newBuilder(TableSchema.findSchema(tableName)).build();
            rows.insert("127.0.0.1", now + i, i, null, BigDecimal.valueOf(i)).finish();

            streamWriter.write(rows);
        }

        CompletableFuture<WriteOk> future = streamWriter.completed();

        WriteOk result = future.get();

        LOG.info("Write result: {}", result);
    }

    @SuppressWarnings("unused")
    private static void runQuery(GreptimeDB greptimeDB, long now) throws Exception {
        QueryRequest request = QueryRequest.newBuilder() //
                .exprType(SelectExprType.Sql) //
                .ql("SELECT * FROM monitor;") //
                .databaseName("public") //
                .build();

        // For performance reasons, the SDK is designed to be purely asynchronous.
        // The return value is a future object. If you want to immediately obtain
        // the result, you can call `future.get()`.
        CompletableFuture<Result<QueryOk, Err>> future = greptimeDB.query(request);

        Result<QueryOk, Err> result = future.get();

        if (result.isOk()) {
            QueryOk queryOk = result.getOk();
            SelectRows rows = queryOk.getRows();
            // `collectToMaps` will discard type information, if type information is needed,
            // please use `collect`.
            List<Map<String, Object>> maps = rows.collectToMaps();
            for (Map<String, Object> map : maps) {
                LOG.info("Query row: {}", map);
            }
        } else {
            LOG.error("Failed to query: {}", result.getErr());
        }
    }

    private static void runPromQueryRange(GreptimeDB greptimeDB, long now) throws Exception {
        // https://docs.greptime.com/user-guide/prometheus#greptimedb-s-http-api
        PromRangeQuery query = PromRangeQuery.newBuildr() //
                .query("sum(monitor)") //
                .start(String.valueOf(now / 1000)) //
                .end(String.valueOf(now / 1000 + 200)) //
                .step("1s") //
                .build();
        QueryRequest request = QueryRequest.newBuilder() //
                .exprType(SelectExprType.Promql) //
                .promQueryRange(query) //
                .databaseName("public") //
                .build();

        // For performance reasons, the SDK is designed to be purely asynchronous.
        // The return value is a future object. If you want to immediately obtain
        // the result, you can call `future.get()`.
        CompletableFuture<Result<QueryOk, Err>> future = greptimeDB.query(request);

        Result<QueryOk, Err> result = future.get();

        if (result.isOk()) {
            QueryOk queryOk = result.getOk();
            SelectRows rows = queryOk.getRows();
            LOG.info("PromQL result: {}", rows.collectToMaps());
        } else {
            LOG.error("Failed to query: {}", result.getErr());
        }
    }
}
