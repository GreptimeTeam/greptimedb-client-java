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
import io.greptime.models.Err;
import io.greptime.models.QueryOk;
import io.greptime.models.QueryRequest;
import io.greptime.models.Result;
import io.greptime.models.SelectExprType;
import io.greptime.options.QueryOptions;
import io.greptime.v1.Columns;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import io.greptime.v1.GreptimeDB;
import io.greptime.v1.codec.Select;
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
public class QueryClientTest {
    private QueryClient  queryClient;
    @Mock
    private RouterClient routerClient;

    @Before
    public void before() {
        QueryOptions queryOpts = new QueryOptions();
        queryOpts.setAsyncPool(ForkJoinPool.commonPool());
        queryOpts.setRouterClient(this.routerClient);

        this.queryClient = new QueryClient();
        this.queryClient.init(queryOpts);
    }

    @After
    public void after() {
        this.queryClient.shutdownGracefully();
        this.routerClient.shutdownGracefully();
    }

    @Test
    public void testQueryOk() throws ExecutionException, InterruptedException {
        Endpoint addr = Endpoint.parse("127.0.0.1:8081");
        Select.SelectResult select = Select.SelectResult.newBuilder() //
            .addColumns(Columns.Column.newBuilder() //
                .setSemanticType(Columns.Column.SemanticType.TAG) //
                .setDataType(Columns.ColumnDataType.STRING) //
                .setValues(Columns.Column.Values.newBuilder() //
                    .addStringValues("tag1") //
                    .addStringValues("tag2") //
                    .addStringValues("tag3") //
                ) //
            ) //
            .addColumns(Columns.Column.newBuilder() //
                .setSemanticType(Columns.Column.SemanticType.FIELD) //
                .setDataType(Columns.ColumnDataType.INT32) //
                .setValues(Columns.Column.Values.newBuilder() //
                    .addI32Values(1) //
                    .addI32Values(2) //
                    .addI32Values(3) //
                ) //
            ) //
            .setRowCount(3) //
            .build();
        GreptimeDB.BatchResponse response = GreptimeDB.BatchResponse.newBuilder() //
            .addDatabases(Database.DatabaseResponse.newBuilder() //
                .addResults(Database.ObjectResult.newBuilder() //
                    .setHeader(Common.ResultHeader.newBuilder().setCode(Status.Success.getStatusCode())) //
                    .setSelect(Database.SelectResult.newBuilder().setRawData(select.toByteString())) //
                )) //
            .build();

        Mockito.when(this.routerClient.route()) //
            .thenReturn(Util.completedCf(addr));
        Mockito.when(this.routerClient.invoke(Mockito.eq(addr), Mockito.any(), Mockito.any())) //
            .thenReturn(Util.completedCf(response));

        QueryRequest req = QueryRequest.newBuilder() //
            .exprType(SelectExprType.Sql) //
            .ql("select * from test") //
            .build();
        Result<QueryOk, Err> res = this.queryClient.query(req).get();

        Assert.assertTrue(res.isOk());
        Assert.assertEquals(3, res.getOk().getRowCount());
    }
}
