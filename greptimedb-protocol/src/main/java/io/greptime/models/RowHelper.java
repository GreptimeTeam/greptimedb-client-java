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

import com.google.protobuf.ByteStringHelper;
import io.greptime.v1.Common;
import io.greptime.v1.RowData;

/**
 * A utility that handles some processing of row based data.
 *
 * @author jiachun.fjc
 */
public final class RowHelper {

    public static void addValue(RowData.Row.Builder builder, //
            Common.ColumnDataType dataType, //
            Common.ColumnDataTypeExtension dataTypeExtension, //
            Object value) {
        RowData.Value.Builder valueBuilder = RowData.Value.newBuilder();
        if (value == null) {
            builder.addValues(valueBuilder.build());
            return;
        }

        switch (dataType) {
            case INT8:
                valueBuilder.setI8Value((int) value);
                break;
            case INT16:
                valueBuilder.setI16Value((int) value);
                break;
            case INT32:
                valueBuilder.setI32Value((int) value);
                break;
            case INT64:
                valueBuilder.setI64Value(Util.getLongValue(value));
                break;
            case UINT8:
                valueBuilder.setU8Value((int) value);
                break;
            case UINT16:
                valueBuilder.setU16Value((int) value);
                break;
            case UINT32:
                valueBuilder.setU32Value((int) value);
                break;
            case UINT64:
                valueBuilder.setU64Value(Util.getLongValue(value));
                break;
            case FLOAT32:
                valueBuilder.setF32Value(((Number) value).floatValue());
                break;
            case FLOAT64:
                valueBuilder.setF64Value(((Number) value).doubleValue());
                break;
            case BOOLEAN:
                valueBuilder.setBoolValue((boolean) value);
                break;
            case BINARY:
                valueBuilder.setBinaryValue(ByteStringHelper.wrap((byte[]) value));
                break;
            case STRING:
                valueBuilder.setStringValue((String) value);
                break;
            case DATE:
                valueBuilder.setDateValue(Util.getDateValue(value));
                break;
            case DATETIME:
                valueBuilder.setDatetimeValue(Util.getDateTimeValue(value));
                break;
            case TIMESTAMP_SECOND:
                valueBuilder.setTimestampSecondValue(Util.getLongValue(value));
                break;
            case TIMESTAMP_MILLISECOND:
                valueBuilder.setTimestampMillisecondValue(Util.getLongValue(value));
                break;
            case TIMESTAMP_NANOSECOND:
                valueBuilder.setTimestampNanosecondValue(Util.getLongValue(value));
                break;
            case TIME_SECOND:
                valueBuilder.setTimeSecondValue(Util.getLongValue(value));
                break;
            case TIME_MILLISECOND:
                valueBuilder.setTimeMillisecondValue(Util.getLongValue(value));
                break;
            case TIME_MICROSECOND:
                valueBuilder.setTimeMicrosecondValue(Util.getLongValue(value));
                break;
            case TIME_NANOSECOND:
                valueBuilder.setTimeNanosecondValue(Util.getLongValue(value));
                break;
            case INTERVAL_YEAR_MONTH:
                valueBuilder.setIntervalYearMonthValue((int) value);
                break;
            case INTERVAL_DAY_TIME:
                valueBuilder.setIntervalDayTimeValue(Util.getLongValue(value));
                break;
            case INTERVAL_MONTH_DAY_NANO:
                valueBuilder.setIntervalMonthDayNanoValue(Util.getIntervalMonthDayNanoValue(value));
                break;
            case DURATION_SECOND:
                valueBuilder.setDurationSecondValue(Util.getLongValue(value));
                break;
            case DURATION_MILLISECOND:
                valueBuilder.setDurationMillisecondValue(Util.getLongValue(value));
                break;
            case DURATION_MICROSECOND:
                valueBuilder.setDurationMicrosecondValue(Util.getLongValue(value));
                break;
            case DURATION_NANOSECOND:
                valueBuilder.setDurationNanosecondValue(Util.getLongValue(value));
                break;
            case DECIMAL128:
                valueBuilder.setDecimal128Value(Util.getDecimal128Value(dataTypeExtension, value));
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported `data_type`: %s", dataType));
        }

        builder.addValues(valueBuilder.build());
    }

    private RowHelper() {}
}
