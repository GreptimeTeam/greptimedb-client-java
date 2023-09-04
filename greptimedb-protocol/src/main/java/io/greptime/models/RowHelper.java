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

    public static void addValue(RowData.Row.Builder builder, Common.ColumnDataType dataType, Object value) {
        RowData.Value.Builder valueBuilder = RowData.Value.newBuilder();
        switch (dataType) {
            case INT8:
                if (value != null) {
                    valueBuilder.setI8Value((int) value);
                }
                break;
            case INT16:
                if (value != null) {
                    valueBuilder.setI16Value((int) value);
                }
                break;
            case INT32:
                if (value != null) {
                    valueBuilder.setI32Value((int) value);
                }
                break;
            case INT64:
                if (value != null) {
                    valueBuilder.setI64Value(Util.getLongValue(value));
                }
                break;
            case UINT8:
                if (value != null) {
                    valueBuilder.setU8Value((int) value);
                }
                break;
            case UINT16:
                if (value != null) {
                    valueBuilder.setU16Value((int) value);
                }
                break;
            case UINT32:
                if (value != null) {
                    valueBuilder.setU32Value((int) value);
                }
                break;
            case UINT64:
                if (value != null) {
                    valueBuilder.setU64Value(Util.getLongValue(value));
                }
                break;
            case FLOAT32:
                if (value != null) {
                    valueBuilder.setF32Value(((Number) value).floatValue());
                }
                break;
            case FLOAT64:
                if (value != null) {
                    valueBuilder.setF64Value(((Number) value).doubleValue());
                }
                break;
            case BOOLEAN:
                if (value != null) {
                    valueBuilder.setBoolValue((boolean) value);
                }
                break;
            case BINARY:
                if (value != null) {
                    valueBuilder.setBinaryValue(ByteStringHelper.wrap((byte[]) value));
                }
                break;
            case STRING:
                if (value != null) {
                    valueBuilder.setStringValue((String) value);
                }
                break;
            case DATE:
                if (value != null) {
                    valueBuilder.setDateValue((int) value);
                }
                break;
            case DATETIME:
                if (value != null) {
                    valueBuilder.setDatetimeValue(Util.getLongValue(value));
                }
                break;
            case TIMESTAMP_SECOND:
                if (value != null) {
                    valueBuilder.setTsSecondValue(Util.getLongValue(value));
                }
                break;
            case TIMESTAMP_MILLISECOND:
                valueBuilder.setTsMillisecondValue(Util.getLongValue(value));
                break;
            case TIMESTAMP_NANOSECOND:
                if (value != null) {
                    valueBuilder.setTsNanosecondValue(Util.getLongValue(value));
                }
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported `data_type`: %s", dataType));
        }
        builder.addValues(valueBuilder.build());
    }

    private RowHelper() {}
}
