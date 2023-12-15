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

import io.greptime.common.Into;
import io.greptime.v1.Common;
import org.apache.arrow.vector.types.pojo.ArrowType;

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
    TimestampNanosecond, //
    TimeSecond, //
    TimeMilliSecond, //
    TimeMicroSecond, //
    TimeNanoSecond, //
    IntervalYearMonth, //
    IntervalDayTime, //
    IntervalMonthDayNano, //
    DurationSecond, //
    DurationMillisecond, //
    DurationMicrosecond, //
    DurationNanosecond, //
    Decimal128, //
    ;

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
            case TimeSecond:
                return Common.ColumnDataType.TIME_SECOND;
            case TimeMilliSecond:
                return Common.ColumnDataType.TIME_MILLISECOND;
            case TimeMicroSecond:
                return Common.ColumnDataType.TIME_MICROSECOND;
            case TimeNanoSecond:
                return Common.ColumnDataType.TIME_NANOSECOND;
            case IntervalYearMonth:
                return Common.ColumnDataType.INTERVAL_YEAR_MONTH;
            case IntervalDayTime:
                return Common.ColumnDataType.INTERVAL_DAY_TIME;
            case IntervalMonthDayNano:
                return Common.ColumnDataType.INTERVAL_MONTH_DAY_NANO;
            case DurationSecond:
                return Common.ColumnDataType.DURATION_SECOND;
            case DurationMillisecond:
                return Common.ColumnDataType.DURATION_MILLISECOND;
            case DurationMicrosecond:
                return Common.ColumnDataType.DURATION_MICROSECOND;
            case DurationNanosecond:
                return Common.ColumnDataType.DURATION_NANOSECOND;
            case Decimal128:
                return Common.ColumnDataType.DECIMAL128;
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
                ArrowType.Timestamp timestampType = (ArrowType.Timestamp) t;
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
            case Interval:
                ArrowType.Interval intervalType = (ArrowType.Interval) t;
                switch (intervalType.getUnit()) {
                    case YEAR_MONTH:
                        return IntervalYearMonth;
                    case DAY_TIME:
                        return IntervalDayTime;
                    case MONTH_DAY_NANO:
                        return IntervalMonthDayNano;
                    default:
                        return null;
                }
            case Duration:
                ArrowType.Duration durationType = (ArrowType.Duration) t;
                switch (durationType.getUnit()) {
                    case SECOND:
                        return DurationSecond;
                    case MILLISECOND:
                        return DurationMillisecond;
                    case MICROSECOND:
                        return DurationMicrosecond;
                    case NANOSECOND:
                        return DurationNanosecond;
                    default:
                        return null;
                }
            case Decimal:
                return Decimal128;
            default:
                return null;
        }
    }

    public static class DecimalTypeExtension implements Into<Common.DecimalTypeExtension> {
        // The maximum precision for [Decimal128] values
        public static final int DECIMAL128_MAX_PRECISION = 38;

        // The maximum scale for [Decimal128] values
        public static final int DECIMAL128_MAX_SCALE = 38;

        public static final DecimalTypeExtension DEFAULT = new DecimalTypeExtension(DECIMAL128_MAX_PRECISION,
                DECIMAL128_MAX_SCALE);

        private final int precision;
        private final int scale;

        public DecimalTypeExtension(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public Common.DecimalTypeExtension into() {
            return Common.DecimalTypeExtension.newBuilder() //
                    .setPrecision(this.precision) //
                    .setScale(this.scale) //
                    .build();
        }
    }
}
