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

/**
 * Data in row format obtained from a query from the DB.
 *
 * @author jiachun.fjc
 */
public class SelectRows implements Iterator<Row> {

    private static final SelectRows EMPTY = new SelectRows(null);

    public static SelectRows empty() {
        return EMPTY;
    }

    private final int                 rowCount;
    private final List<Column>        columns;
    private final Map<String, BitSet> nullMaskCache;
    private int                       cursor = 0;

    public SelectRows(Select.SelectResult result) {
        if (result == null) {
            this.rowCount = 0;
            this.columns = null;
            this.nullMaskCache = Collections.emptyMap();
        } else {
            this.rowCount = result.getRowCount();
            this.columns = result.getColumnsList();
            this.nullMaskCache = new HashMap<>(this.columns.size());
        }
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
