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

import com.codahale.metrics.Histogram;
import io.greptime.Util;
import io.greptime.common.util.Clock;
import io.greptime.rpc.Context;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Data in row format obtained from a query from the DB.
 *
 * @author jiachun.fjc
 */
public interface SelectRows extends Iterator<Row> {

    void produce(VectorSchemaRoot recordbatch) throws InterruptedException;

    default List<Row> collect() {
        Iterable<Row> iterable = () -> this;
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }

    class DefaultSelectRows implements SelectRows {

        private static final Logger LOG = LoggerFactory.getLogger(DefaultSelectRows.class);

        private static final int MAX_CACHED_ROWS = 1024;

        private final Context ctx;
        private final BlockingQueue<Row> rows = new LinkedBlockingQueue<>(MAX_CACHED_ROWS);
        private final Histogram readRowsNum;

        public DefaultSelectRows(Context ctx, Histogram readRowsNum) {
            this.ctx = ctx;
            this.readRowsNum = readRowsNum;
        }

        @Override
        public void produce(VectorSchemaRoot recordbatch) throws InterruptedException {
            List<Field> fields = recordbatch.getSchema().getFields();
            List<FieldVector> vectors = recordbatch.getFieldVectors();

            Long queryId = ctx.get(Context.KEY_QUERY_ID);

            Long queryStart = ctx.get(Context.KEY_QUERY_START);
            if (Util.isRwLogging() && queryStart != null) {
                LOG.info("[Query-{}] First time received data costs {} ms",
                        queryId,
                        Clock.defaultClock().duration(queryStart));
                ctx.remove(Context.KEY_QUERY_START);
            }

            long start = Clock.defaultClock().getTick();

            int index = 0;
            try {
                while (index < recordbatch.getRowCount()) {
                    List<Value> values = new ArrayList<>(vectors.size());
                    for (int i = 0; i < vectors.size(); i++) {
                        Field field = fields.get(i);
                        FieldVector vector = vectors.get(i);

                        values.add(new Value.DefaultValue(
                                field.getName(),
                                ColumnDataType.fromArrowType(field.getType()),
                                vector.getObject(index)));
                    }

                    if (!rows.offer(new Row.DefaultRow(values), 1, TimeUnit.SECONDS)) {
                        throw new RuntimeException(String.format(
                                "[Query-%s] Rows buffer queue is full, please accelerate consumer speed.",
                                queryId));
                    }

                    if (this.readRowsNum != null) {
                        this.readRowsNum.update(1);
                    }

                    index += 1;
                }
            } finally {
                if (Util.isRwLogging()) {
                    LOG.info("[Query-{}] Produce {} rows, costs {} ms",
                            queryId,
                            index,
                            Clock.defaultClock().duration(start)
                    );
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !rows.isEmpty();
        }

        @Override
        public Row next() {
            try {
                return rows.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
