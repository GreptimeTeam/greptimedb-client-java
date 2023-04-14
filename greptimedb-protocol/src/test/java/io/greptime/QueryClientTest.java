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
import io.greptime.models.*;
import io.greptime.options.QueryOptions;
import io.greptime.options.RouterOptions;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertEquals;

/**
 * @author jiachun.fjc
 */
@RunWith(value = MockitoJUnitRunner.class)
public class QueryClientTest {

    static class TestFlightProducer extends NoOpFlightProducer {

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

            Field field1 = new Field("test_column1", FieldType.nullable(new ArrowType.Utf8()), null);
            Field field2 = new Field("test_column2", FieldType.nullable(new ArrowType.Int(32, true)), null);
            Schema schema = new Schema(Arrays.asList(field1, field2));

            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            VectorLoader loader = new VectorLoader(root);
            listener.start(root);

            for (int i = 0; i < 2; i++) {
                VectorSchemaRoot vectors = getTestingVectors(allocator, schema);
                ArrowRecordBatch recordBatch = new VectorUnloader(vectors).getRecordBatch();
                loader.load(recordBatch);

                listener.putNext();
            }

            listener.completed();
        }

        VectorSchemaRoot getTestingVectors(BufferAllocator allocator, Schema schema) {
            VectorSchemaRoot vectors = VectorSchemaRoot.create(schema, allocator);

            VarCharVector column1 = (VarCharVector) vectors.getVector("test_column1");
            column1.allocateNew(3);
            column1.set(0, new Text("tag1"));
            column1.set(1, new Text("tag2"));
            column1.set(2, new Text("tag3"));

            IntVector column2 = (IntVector) vectors.getVector("test_column2");
            column2.allocateNew(3);
            for (int i = 0; i < 3; i++) {
                column2.set(i, i + 1);
            }

            vectors.setRowCount(3);
            return vectors;
        }
    }

    private QueryClient queryClient;
    private RouterClient routerClient;

    @Before
    public void before() {
        RouterOptions opts = new RouterOptions();
        opts.setEndpoints(Collections.singletonList(Endpoint.of("127.0.0.1", 33333)));

        this.routerClient = new RouterClient();
        this.routerClient.init(opts);

        QueryOptions queryOpts = new QueryOptions();
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
    public void testQueryOk() throws ExecutionException, InterruptedException, IOException {
        try (FlightServer flightServer =
                FlightServer.builder(new RootAllocator(Integer.MAX_VALUE),
                        Location.forGrpcInsecure("127.0.0.1", 33333), new TestFlightProducer()).build()) {
            flightServer.start();

            QueryRequest req = QueryRequest.newBuilder() //
                    .exprType(SelectExprType.Sql) //
                    .ql("select * from test") //
                    .build();
            Result<QueryOk, Err> res = this.queryClient.query(req).get();

            Assert.assertTrue(res.isOk());
            List<Row> rows = res.getOk().getRows().collect();
            assertEquals(6, rows.size());

            Row row1 = rows.get(0);
            assertEquals(
                    "[Value{name='test_column1', dataType=String, value=tag1}, Value{name='test_column2', dataType=Int32, value=1}]",
                    row1.values().toString());

            Row row2 = rows.get(1);
            assertEquals(
                    "[Value{name='test_column1', dataType=String, value=tag2}, Value{name='test_column2', dataType=Int32, value=2}]",
                    row2.values().toString());

            Row row3 = rows.get(2);
            assertEquals(
                    "[Value{name='test_column1', dataType=String, value=tag3}, Value{name='test_column2', dataType=Int32, value=3}]",
                    row3.values().toString());
        }
    }
}
