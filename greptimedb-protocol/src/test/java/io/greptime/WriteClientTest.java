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
package io.greptime;

import io.greptime.common.Endpoint;
import io.greptime.models.ColumnDataType;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.SemanticType;
import io.greptime.models.TableName;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.models.WriteRows;
import io.greptime.options.WriteOptions;
import io.greptime.v1.Database;
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
import static io.greptime.models.ColumnDataType.Binary;
import static io.greptime.models.ColumnDataType.Bool;
import static io.greptime.models.ColumnDataType.Date;
import static io.greptime.models.ColumnDataType.DateTime;
import static io.greptime.models.ColumnDataType.Float32;
import static io.greptime.models.ColumnDataType.Float64;
import static io.greptime.models.ColumnDataType.Int16;
import static io.greptime.models.ColumnDataType.Int32;
import static io.greptime.models.ColumnDataType.Int64;
import static io.greptime.models.ColumnDataType.Int8;
import static io.greptime.models.ColumnDataType.TimestampMillisecond;
import static io.greptime.models.ColumnDataType.TimestampNanosecond;
import static io.greptime.models.ColumnDataType.TimestampSecond;
import static io.greptime.models.ColumnDataType.UInt16;
import static io.greptime.models.ColumnDataType.UInt32;
import static io.greptime.models.ColumnDataType.UInt64;
import static io.greptime.models.ColumnDataType.UInt8;
import static io.greptime.models.SemanticType.Field;
import static io.greptime.models.SemanticType.Tag;
import static io.greptime.models.SemanticType.Timestamp;

/**
 * @author jiachun.fjc
 */
@RunWith(value = MockitoJUnitRunner.class)
public class WriteClientTest {
    private WriteClient writeClient;
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
        String[] columnNames =
                new String[] {"test_tag", "test_ts", "field1", "field2", "field3", "field4", "field5", "field6",
                        "field7", "field8", "field9", "field10", "field11", "field12", "field13", "field14", "field15",
                        "field16", "field17"};
        SemanticType[] semanticTypes =
                new SemanticType[] {Tag, Timestamp, Field, Field, Field, Field, Field, Field, Field, Field, Field,
                        Field, Field, Field, Field, Field, Field, Field, Field};
        ColumnDataType[] dataTypes =
                new ColumnDataType[] {ColumnDataType.String, Int64, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32,
                        UInt64, Float32, Float64, Bool, Binary, Date, DateTime, TimestampSecond, TimestampMillisecond,
                        TimestampNanosecond};

        TableSchema schema = TableSchema.newBuilder(TableName.with("", "test_table")) //
                .columnNames(columnNames) //
                .semanticTypes(semanticTypes) //
                .dataTypes(dataTypes) //
                .build();
        WriteRows rows = WriteRows.newBuilder(schema).build();
        long ts = System.currentTimeMillis();

        rows.insert("tag1", ts, 1, 2, 3, 4L, 5, 6, 7, 8L, 0.9F, 0.10D, true, new byte[0], 11, 12L, 13L, 14L, 15L);
        rows.insert("tag1", ts, 1, 2, 3, 4, 5, 6, 7, 8, 0.9, 0.10, false, new byte[0], 11, 12, 13, 14, 15);
        rows.insert("tag1", ts, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, false, new byte[] {0, 1}, 11, 12, 13, 14, 15);

        rows.finish();

        Endpoint addr = Endpoint.parse("127.0.0.1:8081");
        Database.GreptimeResponse response = Database.GreptimeResponse.newBuilder() //
                .setAffectedRows(Database.AffectedRows.newBuilder().setValue(3)) //
                .build();

        Mockito.when(this.routerClient.route()) //
                .thenReturn(Util.completedCf(addr));
        Mockito.when(this.routerClient.invoke(Mockito.eq(addr), Mockito.any(), Mockito.any())) //
                .thenReturn(Util.completedCf(response));

        Result<WriteOk, Err> res = this.writeClient.write(rows).get();

        Assert.assertTrue(res.isOk());
        Assert.assertEquals(3, res.getOk().getSuccess());
    }
}
