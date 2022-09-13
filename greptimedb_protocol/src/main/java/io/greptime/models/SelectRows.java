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

import io.greptime.v1.Columns.Column;
import io.greptime.v1.codec.Select;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Data in row format obtained from a query from the DB.
 *
 * @author jiachun.fjc
 */
public interface SelectRows extends Iterator<Row> {
    int rowCount();

    default List<Row> collect() {
        Iterable<Row> iterable = () -> this;
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }

    static SelectRows empty() {
        return DefaultSelectRows.EMPTY;
    }

    static SelectRows from(Select.SelectResult res) {
        return new DefaultSelectRows(res);
    }

    class DefaultSelectRows implements SelectRows {

        private static final DefaultSelectRows EMPTY  = new DefaultSelectRows(null);

        private final int                      rowCount;
        private final List<Column>             columns;
        private final Map<String, BitSet>      nullMaskCache;
        private int                            cursor = 0;

        DefaultSelectRows(Select.SelectResult res) {
            if (res == null) {
                this.rowCount = 0;
                this.columns = null;
                this.nullMaskCache = Collections.emptyMap();
            } else {
                this.rowCount = res.getRowCount();
                this.columns = res.getColumnsList();
                this.nullMaskCache = new HashMap<>(this.columns.size());
            }
        }

        @Override
        public int rowCount() {
            return rowCount;
        }

        @Override
        public boolean hasNext() {
            return this.cursor < rowCount;
        }

        @Override
        public Row next() {
            int index = this.cursor++;
            List<Value> values = this.columns.stream() //
                .map(column -> new Value() {
                    @Override
                    public String name() {
                        return column.getColumnName();
                    }

                    @Override
                    public SemanticType semanticType() {
                        return SemanticType.fromProtoValue(column.getSemanticType());
                    }

                    @Override
                    public ColumnDataType dataType() {
                        return ColumnDataType.fromProtoValue(ColumnHelper.getValueType(column));
                    }

                    @Override
                    public Object value() {
                        return getColumnValue(column, index);
                    }

                    @Override
                    public String toString() {
                        return "Value{" + //
                                "name='" + name() + '\'' + //
                                ", semanticType=" + semanticType() + //
                                ", dataType=" + dataType() + //
                                ", value=" + value() + //
                                '}';
                    }
                })
                .collect(Collectors.toList());

            return () -> values;
        }

        private Object getColumnValue(Column column, int index) {
            Function<String, BitSet> func = k -> ColumnHelper.getNullMaskBits(column);
            BitSet nullMask = this.nullMaskCache.computeIfAbsent(column.getColumnName(), func);
            return ColumnHelper.getValue(column, index, nullMask);
        }
    }
}
