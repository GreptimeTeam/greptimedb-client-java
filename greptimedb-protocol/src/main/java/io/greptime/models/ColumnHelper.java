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

import com.google.protobuf.ByteStringHelper;
import com.google.protobuf.Descriptors;
import io.greptime.common.util.Ensures;
import io.greptime.v1.Columns;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * A utility that handles some processing of column data.
 *
 * @author jiachun.fjc
 */
public final class ColumnHelper {

    private static final Map<Columns.ColumnDataType, String> COLUMN_TYPES_DICT;

    static {
        COLUMN_TYPES_DICT = new HashMap<>();
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.INT8, "i8_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.INT16, "i16_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.INT32, "i32_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.INT64, "i64_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.UINT8, "u8_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.UINT16, "u16_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.UINT32, "u32_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.UINT64, "u64_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.FLOAT32, "f32_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.FLOAT64, "f64_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.BOOLEAN, "bool_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.BINARY, "binary_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.STRING, "string_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.DATE, "date_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.DATETIME, "datetime_values");
        COLUMN_TYPES_DICT.put(Columns.ColumnDataType.TIMESTAMP, "ts_millis_values");
    }

    public static Columns.ColumnDataType getValueType(Columns.Column column) {
        return column.getDatatype();
    }

    public static void addToColumnValuesBuilder(Columns.Column.Builder builder, Object value) {
        Columns.ColumnDataType dataType = builder.getDatatype();
        String fieldName = COLUMN_TYPES_DICT.get(dataType);

        Ensures.ensureNonNull(fieldName, "Unsupported `data_type`: %s", dataType);

        Columns.Column.Values.Builder valuesBuilder = builder.getValuesBuilder();
        Descriptors.FieldDescriptor fd = valuesBuilder.getDescriptorForType().findFieldByName(fieldName);
        valuesBuilder.addRepeatedField(fd, value);
    }

    public static Object getValue(Columns.Column column, int index, BitSet nullMask) {
        Columns.Column.Values values = column.getValues();
        Descriptors.FieldDescriptor fd = getValueFd(column);
        if (nullMask.isEmpty()) {
            return values.getRepeatedField(fd, index);
        }

        Ensures.ensure(index < nullMask.size());

        if (nullMask.get(index)) {
            return null;
        }

        int cardinality = nullMask.get(0, index).cardinality();
        return values.getRepeatedField(fd, index - cardinality);
    }

    public static BitSet getNullMaskBits(Columns.Column column) {
        return BitSet.valueOf(ByteStringHelper.sealByteArray(column.getNullMask()));
    }

    private static Descriptors.FieldDescriptor getValueFd(Columns.Column column) {
        Columns.ColumnDataType dataType = column.getDatatype();
        String fieldName = COLUMN_TYPES_DICT.get(dataType);

        Ensures.ensureNonNull(fieldName, "Unsupported `data_type`: %s", dataType);

        return column.getValues().getDescriptorForType().findFieldByName(fieldName);
    }

    private ColumnHelper() {
    }
}
