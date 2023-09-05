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

import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;

/**
 * @author jiachun.fjc
 */
public class Util {

    static int ONE_DAY_IN_SECONDS = 86400;

    public static long getLongValue(Object value) {
        if (value instanceof Integer) {
            return (int) value;
        } else if (value instanceof Long) {
            return (long) value;
        }
        return ((Number) value).longValue();
    }

    public static int getDataValue(Object value) {
        if (value instanceof Date) {
            Instant instant = ((Date) value).toInstant();
            long epochDay = instant.getEpochSecond() / ONE_DAY_IN_SECONDS;
            return (int) epochDay;
        } else if (value instanceof LocalDate) {
            return (int) ((LocalDate) value).toEpochDay();
        }
        return (int) getLongValue(value);
    }

    public static int getDataTimeValue(Object value) {
        if (value instanceof Date) {
            Instant instant = ((Date) value).toInstant();
            long epochDay = instant.getEpochSecond();
            return (int) epochDay;
        }
        return (int) getLongValue(value);
    }
}
