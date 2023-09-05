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

import io.greptime.v1.Common;
import org.apache.arrow.vector.types.pojo.ArrowType;
import static org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;

/**
 * Column data type.
 *
 * @author jiachun.fjc
 */
public enum ColumnDataType {
    Bool, //
    Int8, //
    Int16, //
    Int32, //
    Int64, //
    UInt8, //
    UInt16, //
    UInt32, //
    UInt64, //
    Float32, //
    Float64, //
    Binary, //
    String, //
    Date, //
    DateTime, //
    TimestampSecond, //
    TimestampMillisecond, //
    TimestampMicrosecond, //
    TimestampNanosecond;

    public Common.ColumnDataType toProtoValue() {
        switch (this) {
            case Bool:
                return Common.ColumnDataType.BOOLEAN;
            case Int8:
                return Common.ColumnDataType.INT8;
            case Int16:
                return Common.ColumnDataType.INT16;
            case Int32:
                return Common.ColumnDataType.INT32;
            case Int64:
                return Common.ColumnDataType.INT64;
            case UInt8:
                return Common.ColumnDataType.UINT8;
            case UInt16:
                return Common.ColumnDataType.UINT16;
            case UInt32:
                return Common.ColumnDataType.UINT32;
            case UInt64:
                return Common.ColumnDataType.UINT64;
            case Float32:
                return Common.ColumnDataType.FLOAT32;
            case Float64:
                return Common.ColumnDataType.FLOAT64;
            case Binary:
                return Common.ColumnDataType.BINARY;
            case String:
                return Common.ColumnDataType.STRING;
            case Date:
                return Common.ColumnDataType.DATE;
            case DateTime:
                return Common.ColumnDataType.DATETIME;
            case TimestampSecond:
                return Common.ColumnDataType.TIMESTAMP_SECOND;
            case TimestampMillisecond:
                return Common.ColumnDataType.TIMESTAMP_MILLISECOND;
            case TimestampMicrosecond:
                return Common.ColumnDataType.TIMESTAMP_MICROSECOND;
            case TimestampNanosecond:
                return Common.ColumnDataType.TIMESTAMP_NANOSECOND;
            default:
                return null;
        }
    }

    public static ColumnDataType fromArrowType(ArrowType t) {
        switch (t.getTypeID()) {
            case Bool:
                return Bool;
            case Int:
                return Int32;
            case FloatingPoint:
                return Float64;
            case Binary:
            case LargeBinary:
                return Binary;
            case Utf8:
            case LargeUtf8:
                return String;
            case Date:
                ArrowType.Date dateType = (ArrowType.Date) t;
                switch (dateType.getUnit()) {
                    case DAY:
                        return Date;
                    case MILLISECOND:
                        return DateTime;
                    default:
                        return null;
                }
            case Timestamp:
                Timestamp timestampType = (Timestamp) t;
                switch (timestampType.getUnit()) {
                    case SECOND:
                        return TimestampSecond;
                    case MILLISECOND:
                        return TimestampMillisecond;
                    case MICROSECOND:
                        return TimestampMicrosecond;
                    case NANOSECOND:
                        return TimestampNanosecond;
                    default:
                        return null;
                }
            default:
                return null;
        }
    }
}
