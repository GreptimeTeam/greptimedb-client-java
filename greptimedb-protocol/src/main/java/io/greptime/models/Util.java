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

import io.greptime.common.util.Ensures;
import io.greptime.v1.Common;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;

/**
 * @author jiachun.fjc
 */
public class Util {

    static int ONE_DAY_IN_SECONDS = 86400;

    static long getLongValue(Object value) {
        if (value instanceof Integer) {
            return (int) value;
        }

        if (value instanceof Long) {
            return (long) value;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        // Not null
        throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
    }

    static int getDateValue(Object value) {
        if (value instanceof Instant) {
            long epochDay = ((Instant) value).getEpochSecond() / ONE_DAY_IN_SECONDS;
            return (int) epochDay;
        }

        if (value instanceof Date) {
            Instant instant = ((Date) value).toInstant();
            long epochDay = instant.getEpochSecond() / ONE_DAY_IN_SECONDS;
            return (int) epochDay;
        }

        if (value instanceof LocalDate) {
            return (int) ((LocalDate) value).toEpochDay();
        }

        return (int) getLongValue(value);
    }

    static int getDateTimeValue(Object value) {
        if (value instanceof Instant) {
            long epochSecond = ((Instant) value).getEpochSecond();
            return (int) epochSecond;
        }

        if (value instanceof Date) {
            Instant instant = ((Date) value).toInstant();
            long epochSecond = instant.getEpochSecond();
            return (int) epochSecond;
        }

        return (int) getLongValue(value);
    }

    static Common.IntervalMonthDayNano getIntervalMonthDayNanoValue(Object value) {
        Ensures.ensure(value instanceof IntervalMonthDayNano, "Expected type: `IntervalMonthDayNano`, actual: %s",
                value.getClass());
        return ((IntervalMonthDayNano) value).into();
    }

    static Common.Decimal128 getDecimal128Value(Common.ColumnDataTypeExtension dataTypeExtension, Object value) {
        Ensures.ensure(value instanceof BigDecimal, "Expected type: `BigDecimal`, actual: %s", value.getClass());
        Ensures.ensureNonNull(dataTypeExtension, "Null `dataTypeExtension`");
        Common.DecimalTypeExtension decimalTypeExtension =
                dataTypeExtension.hasDecimalType() ? dataTypeExtension.getDecimalType()
                        : ColumnDataType.DecimalTypeExtension.DEFAULT.into();
        BigDecimal decimal = (BigDecimal) value;
        BigDecimal converted = decimal.setScale(decimalTypeExtension.getScale(), RoundingMode.HALF_UP);

        BigInteger unscaledValue = converted.unscaledValue();
        long high64Bits = unscaledValue.shiftRight(64).longValue();
        long low64Bits = unscaledValue.longValue();

        return Common.Decimal128.newBuilder().setHi(high64Bits).setLo(low64Bits).build();
    }
}
