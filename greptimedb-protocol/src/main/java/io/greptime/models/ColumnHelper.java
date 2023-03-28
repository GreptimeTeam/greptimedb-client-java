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
import io.greptime.common.util.Ensures;
import io.greptime.v1.Columns;
import java.util.BitSet;

/**
 * A utility that handles some processing of column data.
 *
 * @author jiachun.fjc
 */
public final class ColumnHelper {

    public static void addToColumnValuesBuilder(Columns.Column.Builder builder, Object value) {
        Columns.Column.Values.Builder valuesBuilder = builder.getValuesBuilder();
        Columns.ColumnDataType dataType = builder.getDatatype();
        addValue(valuesBuilder, dataType, value);
    }

    public static Object getValue(Columns.Column column, int index, BitSet nullMask) {
        Columns.Column.Values values = column.getValues();
        Columns.ColumnDataType dataType = column.getDatatype();
        if (nullMask.isEmpty()) {
            return getValue(values, dataType, index);
        }

        Ensures.ensure(index < nullMask.size(), "Index out of range: %d", index);

        if (nullMask.get(index)) {
            return null;
        }

        int cardinality = nullMask.get(0, index).cardinality();
        return getValue(values, dataType, index - cardinality);
    }

    public static BitSet getNullMaskBits(Columns.Column column) {
        return BitSet.valueOf(ByteStringHelper.sealByteArray(column.getNullMask()));
    }

    private static void addValue(Columns.Column.Values.Builder builder, Columns.ColumnDataType dataType, Object value) {
        switch (dataType) {
            case INT8:
                builder.addI8Values((int) value);
                break;
            case INT16:
                builder.addI16Values((int) value);
                break;
            case INT32:
                builder.addI32Values((int) value);
                break;
            case INT64:
                builder.addI64Values(getLongValue(value));
                break;
            case UINT8:
                builder.addU8Values((int) value);
                break;
            case UINT16:
                builder.addU16Values((int) value);
                break;
            case UINT32:
                builder.addU32Values((int) value);
                break;
            case UINT64:
                builder.addU64Values(getLongValue(value));
                break;
            case FLOAT32:
                builder.addF32Values(((Number) value).floatValue());
                break;
            case FLOAT64:
                builder.addF64Values(((Number) value).doubleValue());
                break;
            case BOOLEAN:
                builder.addBoolValues((boolean) value);
                break;
            case BINARY:
                builder.addBinaryValues(ByteStringHelper.wrap((byte[]) value));
                break;
            case STRING:
                builder.addStringValues((String) value);
                break;
            case DATE:
                builder.addDateValues((int) value);
                break;
            case DATETIME:
                builder.addDatetimeValues(getLongValue(value));
                break;
            case TIMESTAMP_SECOND:
                builder.addTsSecondValues(getLongValue(value));
                break;
            case TIMESTAMP_MILLISECOND:
                builder.addTsMillisecondValues(getLongValue(value));
                break;
            case TIMESTAMP_NANOSECOND:
                builder.addTsNanosecondValues(getLongValue(value));
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported `data_type`: %s", dataType));
        }
    }

    private static Object getValue(Columns.Column.Values values, Columns.ColumnDataType dataType, int index) {
        switch (dataType) {
            case INT8:
                return values.getI8Values(index);
            case INT16:
                return values.getI16Values(index);
            case INT32:
                return values.getI32Values(index);
            case INT64:
                return values.getI64Values(index);
            case UINT8:
                return values.getU8Values(index);
            case UINT16:
                return values.getU16Values(index);
            case UINT32:
                return values.getU32Values(index);
            case UINT64:
                return values.getU64Values(index);
            case FLOAT32:
                return values.getF32Values(index);
            case FLOAT64:
                return values.getF64Values(index);
            case BOOLEAN:
                return values.getBoolValues(index);
            case BINARY:
                return values.getBinaryValues(index);
            case STRING:
                return values.getStringValues(index);
            case DATE:
                return values.getDateValues(index);
            case DATETIME:
                return values.getDatetimeValues(index);
            case TIMESTAMP_SECOND:
                return values.getTsSecondValues(index);
            case TIMESTAMP_MILLISECOND:
                return values.getTsMillisecondValues(index);
            case TIMESTAMP_NANOSECOND:
                return values.getTsNanosecondValues(index);
            default:
                throw new IllegalArgumentException(String.format("Unsupported `data_type`: %s", dataType));
        }
    }

    private static long getLongValue(Object value) {
        if (value instanceof Integer) {
            return (int) value;
        } else if (value instanceof Long) {
            return (long) value;
        }
        return ((Number) value).longValue();
    }

    private ColumnHelper() {}
}
