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

import com.google.protobuf.InvalidProtocolBufferException;
import io.greptime.common.Endpoint;
import io.greptime.models.ColumnDataType;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.SemanticType;
import io.greptime.models.TableName;
import io.greptime.models.WriteOk;
import io.greptime.models.WriteRows;
import io.greptime.options.RouterOptions;
import io.greptime.options.WriteOptions;
import io.greptime.v1.Database;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

/**
 * @author jiachun.fjc
 */
@RunWith(value = MockitoJUnitRunner.class)
public class WriteClientTest {

    static class TestFlightProducer extends NoOpFlightProducer {

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

            byte[] bytes = ticket.getBytes();
            Database.GreptimeRequest request;
            try {
                request = Database.GreptimeRequest.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            Database.InsertRequest insert = request.getInsert();

            Database.FlightMetadata.Builder builder = Database.FlightMetadata.newBuilder();
            builder.setAffectedRows(Database.AffectedRows.newBuilder().setValue(insert.getRowCount()).build());
            Database.FlightMetadata flightMetadata = builder.build();
            byte[] rawMetadata = flightMetadata.toByteArray();

            ArrowBuf buffer = allocator.buffer(rawMetadata.length);
            buffer.writeBytes(rawMetadata);
            listener.putMetadata(buffer);
            listener.completed();
        }
    }

    private WriteClient writeClient;
    private RouterClient routerClient;

    @Before
    public void before() {
        RouterOptions opts = new RouterOptions();
        opts.setEndpoints(Collections.singletonList(Endpoint.of("127.0.0.1", 44444)));

        routerClient = new RouterClient();
        routerClient.init(opts);

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
    public void testWriteSuccess() throws ExecutionException, InterruptedException, IOException {
        FlightServer flightServer =
                FlightServer.builder(new RootAllocator(Integer.MAX_VALUE),
                        Location.forGrpcInsecure("127.0.0.1", 44444), new TestFlightProducer()).build();
        flightServer.start();

        WriteRows rows = WriteRows.newBuilder(TableName.with("", "test_table")) //
                .columnNames("test_tag", "test_ts", "test_field") //
                .semanticTypes(SemanticType.Tag, SemanticType.Timestamp, SemanticType.Field) //
                .dataTypes(ColumnDataType.String, ColumnDataType.Int64, ColumnDataType.Float64) //
                .build();

        rows.insert("tag1", System.currentTimeMillis(), 0.1);
        rows.insert("tag2", System.currentTimeMillis(), 0.2);
        rows.insert("tag3", System.currentTimeMillis(), 0.3);

        rows.finish();

        Result<WriteOk, Err> res = this.writeClient.write(rows).get();
        Assert.assertTrue(res.isOk());
        Assert.assertEquals(3, res.getOk().getSuccess());
    }
}
