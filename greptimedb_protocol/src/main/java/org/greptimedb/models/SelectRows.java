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
package org.greptimedb.models;

import com.google.protobuf.ByteStringHelper;
import org.greptimedb.v1.ColumnUtil;
import org.greptimedb.v1.Columns.Column;
import org.greptimedb.v1.Columns.Column.SemanticType;
import org.greptimedb.v1.codec.Select;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 *
 * @author jiachun.fjc
 */
public class SelectRows implements Iterator<Row> {

    private static final SelectRows EMPTY = new SelectRows(null);

    public static SelectRows empty() {
        return EMPTY;
    }

    private final List<Column>        columns;
    private final int                 rowCount;
    private final Map<String, BitSet> nullMaskCache = new HashMap<>();
    private int                       cursor        = 0;

    public SelectRows(Select.SelectResult result) {
        this.columns = result == null ? null : result.getColumnsList();
        this.rowCount = result == null ? 0 : result.getRowCount();
    }

    @Override
    public boolean hasNext() {
        return this.cursor < rowCount;
    }

    @Override
    public Row next() {
        List<Value> values = new ArrayList<>(this.columns.size());
        for (Column column: this.columns) {
            values.add(new Value() {

                @Override
                public String name() {
                    return column.getColumnName();
                }

                @Override
                public SemanticType semanticType() {
                    return column.getSemanticType();
                }

                @Override
                public Type valueType() {
                    return ColumnUtil.getValueType(column);
                }

                @Override
                public Object value() {
                    byte[] nullMaskBytes = ByteStringHelper.sealByteArray(column.getNullMask());
                    Function<String, BitSet> func = k -> BitSet.valueOf(nullMaskBytes);
                    BitSet nullMask = nullMaskCache.computeIfAbsent(column.getColumnName(), func);
                    return ColumnUtil.getValue(column, cursor, nullMask);
                }
            });
        }
        this.cursor++;

        return () -> values;
    }
}
