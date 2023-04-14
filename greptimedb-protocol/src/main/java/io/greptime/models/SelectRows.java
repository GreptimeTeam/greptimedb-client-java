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
package io.greptime.models;

import com.codahale.metrics.Histogram;
import io.greptime.Util;
import io.greptime.common.Endpoint;
import io.greptime.common.util.Clock;
import io.greptime.rpc.Context;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Data in row format obtained from a query from the DB.
 *
 * @author jiachun.fjc
 */
public interface SelectRows extends Iterator<Row> {

    /**
     * @return true if it's ready to be polled for data (calling `next` for `Row`s).
     */
    boolean isReady();

    void close();

    default List<Row> collect() {
        Iterable<Row> iterable = () -> this;
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }

    default List<Map<String, Object>> collectToMaps() {
        Iterable<Row> iterable = () -> this;
        return StreamSupport.stream(iterable.spliterator(), false)
                .map(row -> {
                    Map<String, Object> map = new HashMap<>();
                    for (Value v: row.values()) {
                        map.put(v.name(), v.value());
                    }
                    return map;
                })
                .collect(Collectors.toList());
    }

    class DefaultSelectRows implements SelectRows {

        private static final Logger LOG = LoggerFactory.getLogger(DefaultSelectRows.class);

        private final Context ctx;
        private final Queue<Row> rows = new ConcurrentLinkedQueue<>();
        private final Histogram readRowsNum;
        private final FlightStream flightStream;

        public DefaultSelectRows(Context ctx, Histogram readRowsNum, FlightStream flightStream) {
            this.ctx = ctx;
            this.readRowsNum = readRowsNum;
            this.flightStream = flightStream;
        }

        void consume(VectorSchemaRoot recordBatch) {
            List<Field> fields = recordBatch.getSchema().getFields();
            List<FieldVector> vectors = recordBatch.getFieldVectors();

            Long queryId = this.ctx.get(Context.KEY_QUERY_ID);
            Long queryStart = this.ctx.remove(Context.KEY_QUERY_START);
            if (Util.isRwLogging() && queryStart != null) {
                Endpoint endpoint = this.ctx.get(Context.KEY_ENDPOINT);
                LOG.info("[Query-{}] First time consuming data from {}, costs {} ms",
                        queryId, endpoint, Clock.defaultClock().duration(queryStart));
            }

            long start = Clock.defaultClock().getTick();

            int index = 0;
            try {
                while (index < recordBatch.getRowCount()) {
                    List<Value> values = new ArrayList<>(vectors.size());
                    for (int i = 0; i < vectors.size(); i++) {
                        Field field = fields.get(i);
                        FieldVector vector = vectors.get(i);

                        values.add(new Value.DefaultValue(
                                field.getName(),
                                ColumnDataType.fromArrowType(field.getType()),
                                vector.getObject(index)));
                    }

                    this.rows.offer(new Row.DefaultRow(values));

                    index += 1;
                }
            } finally {
                if (this.readRowsNum != null) {
                    this.readRowsNum.update(index);
                }
                if (Util.isRwLogging()) {
                    Endpoint endpoint = this.ctx.get(Context.KEY_ENDPOINT);
                    LOG.info("[Query-{}] Consume {} rows from {}, costs {} ms",
                            queryId, index, endpoint, Clock.defaultClock().duration(start));
                }
            }
        }

        @Override
        public boolean isReady() {
            return this.flightStream.hasRoot();
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = !this.rows.isEmpty();
            if (hasNext) {
                return true;
            }

            try {
                hasNext = this.flightStream.next();
                if (hasNext) {
                    try (VectorSchemaRoot recordBatch = this.flightStream.getRoot()) {
                        consume(recordBatch);
                    }
                }
            } finally {
                if (!hasNext) {
                    close();
                }
            }
            return hasNext;
        }

        @Override
        public Row next() {
            return this.rows.poll();
        }

        @Override
        public void close() {
            try {
                this.flightStream.close();
            } catch (Exception e) {
                LOG.error("Failed to close `FlightStream`", e);
            }
        }
    }
}
