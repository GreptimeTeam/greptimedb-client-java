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

/**
 * A utility that handles some processing of column data.
 *
 * @author jiachun.fjc
 */
public final class ColumnUtil {

    private static final String[]     COLUMN_VALUE_NAME_INDEX;
    private static final Value.Type[] COLUMN_VALUE_TYPE_INDEX;

    static {
        COLUMN_VALUE_NAME_INDEX = new String[13];
        COLUMN_VALUE_NAME_INDEX[0] = "i8_values";
        COLUMN_VALUE_NAME_INDEX[1] = "i16_values";
        COLUMN_VALUE_NAME_INDEX[2] = "i32_values";
        COLUMN_VALUE_NAME_INDEX[3] = "i64_values";
        COLUMN_VALUE_NAME_INDEX[4] = "u8_values";
        COLUMN_VALUE_NAME_INDEX[5] = "u16_values";
        COLUMN_VALUE_NAME_INDEX[6] = "u32_values";
        COLUMN_VALUE_NAME_INDEX[7] = "u64_values";
        COLUMN_VALUE_NAME_INDEX[8] = "f32_values";
        COLUMN_VALUE_NAME_INDEX[9] = "f64_values";
        COLUMN_VALUE_NAME_INDEX[10] = "bool_values";
        COLUMN_VALUE_NAME_INDEX[11] = "binary_values";
        COLUMN_VALUE_NAME_INDEX[12] = "string_values";

        COLUMN_VALUE_TYPE_INDEX = new Value.Type[13];
        COLUMN_VALUE_TYPE_INDEX[0] = Value.Type.Int32;
        COLUMN_VALUE_TYPE_INDEX[1] = Value.Type.Int32;
        COLUMN_VALUE_TYPE_INDEX[2] = Value.Type.Int32;
        COLUMN_VALUE_TYPE_INDEX[3] = Value.Type.Int64;
        COLUMN_VALUE_TYPE_INDEX[4] = Value.Type.UInt32;
        COLUMN_VALUE_TYPE_INDEX[5] = Value.Type.UInt32;
        COLUMN_VALUE_TYPE_INDEX[6] = Value.Type.UInt32;
        COLUMN_VALUE_TYPE_INDEX[7] = Value.Type.UInt64;
        COLUMN_VALUE_TYPE_INDEX[8] = Value.Type.Float;
        COLUMN_VALUE_TYPE_INDEX[9] = Value.Type.Double;
        COLUMN_VALUE_TYPE_INDEX[10] = Value.Type.Bool;
        COLUMN_VALUE_TYPE_INDEX[11] = Value.Type.Bytes;
        COLUMN_VALUE_TYPE_INDEX[12] = Value.Type.String;
    }

    public static Value.Type getValueType(Columns.Column column) {
        Ensures.ensure(column.hasValueIndex(), "`value_index` is required");
        int valueIndex = column.getValueIndex();
        Ensures.ensure(valueIndex < COLUMN_VALUE_TYPE_INDEX.length, "value_index overflow: %d", valueIndex);
        return COLUMN_VALUE_TYPE_INDEX[valueIndex];
    }

    public static Object getValue(Columns.Column column, int index, BitSet nullMask) {
        Ensures.ensure(column.hasValueIndex(), "`value_index` is required");
        int valueIndex = column.getValueIndex();
        Ensures.ensure(valueIndex < COLUMN_VALUE_NAME_INDEX.length, "value_index overflow: %d", index);
        String fieldName = COLUMN_VALUE_NAME_INDEX[valueIndex];
        Columns.Column.Values values = column.getValues();
        Descriptors.FieldDescriptor fd = values.getDescriptorForType().findFieldByName(fieldName);
        if (nullMask.isEmpty()) {
            return values.getRepeatedField(fd, index);
        }

        Ensures.ensure(index < nullMask.size());

        if (nullMask.get(index)) {
            return null;
        }

        int cardinality = 0;
        for (int i = 0; i <= index; i++) {
            if (nullMask.get(i)) {
                cardinality++;
            }
        }
        return values.getRepeatedField(fd, index - cardinality);
    }

    public static BitSet getNullMaskBits(Columns.Column column) {
        return BitSet.valueOf(ByteStringHelper.sealByteArray(column.getNullMask()));
    }

    private ColumnUtil() {
    }
}
