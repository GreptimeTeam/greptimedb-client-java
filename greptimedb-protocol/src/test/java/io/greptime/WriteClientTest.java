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

import io.greptime.common.Endpoint;
import io.greptime.models.ColumnDataType;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.SemanticType;
import io.greptime.models.TableName;
import io.greptime.models.WriteOk;
import io.greptime.models.WriteRows;
import io.greptime.options.WriteOptions;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import io.greptime.v1.GreptimeDB;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

/**
 * @author jiachun.fjc
 */
@RunWith(value = MockitoJUnitRunner.class)
public class WriteClientTest {
    private WriteClient  writeClient;
    @Mock
    private RouterClient routerClient;

    @Before
    public void before() {
        WriteOptions writeOpts = new WriteOptions();
        writeOpts.setAsyncPool(ForkJoinPool.commonPool());
        writeOpts.setRouterClient(this.routerClient);

        this.writeClient = new WriteClient();
        this.writeClient.init(writeOpts);
    }

    @After
    public void after() {
        this.writeClient.shutdownGracefully();
        this.routerClient.shutdownGracefully();
    }

    @Test
    public void testWriteSuccess() throws ExecutionException, InterruptedException {
        WriteRows rows = WriteRows.newBuilder(TableName.with("", "test_table")) //
            .columnNames("test_tag", "test_ts", "test_field") //
            .semanticTypes(SemanticType.Tag, SemanticType.Timestamp, SemanticType.Field) //
            .dataTypes(ColumnDataType.String, ColumnDataType.Int64, ColumnDataType.Float64) //
            .build();

        rows.insert("tag1", System.currentTimeMillis(), 0.1);
        rows.insert("tag2", System.currentTimeMillis(), 0.2);
        rows.insert("tag3", System.currentTimeMillis(), 0.3);

        rows.finish();

        Endpoint addr = Endpoint.parse("127.0.0.1:8081");
        GreptimeDB.BatchResponse response = GreptimeDB.BatchResponse.newBuilder() //
            .addDatabases(Database.DatabaseResponse.newBuilder() //
                .addResults(Database.ObjectResult.newBuilder() //
                    .setHeader(Common.ResultHeader.newBuilder().setCode(Status.Success.getStatusCode())) //
                    .setMutate(Common.MutateResult.newBuilder().setSuccess(3)) //
                )) //
            .build();

        Mockito.when(this.routerClient.route()) //
            .thenReturn(Util.completedCf(addr));
        Mockito.when(this.routerClient.invoke(Mockito.eq(addr), Mockito.any(), Mockito.any())) //
            .thenReturn(Util.completedCf(response));

        Result<WriteOk, Err> res = this.writeClient.write(rows).get();

        Assert.assertTrue(res.isOk());
    }
}
