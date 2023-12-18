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

/**
 * @author jiachun.fjc
 */
public class IntervalMonthDayNano implements Into<Common.IntervalMonthDayNano> {
    private final int months;
    private final int days;
    private final long nanoseconds;

    public IntervalMonthDayNano(int months, int days, long nanoseconds) {
        this.months = months;
        this.days = days;
        this.nanoseconds = nanoseconds;
    }

    public int getMonths() {
        return months;
    }

    public int getDays() {
        return days;
    }

    public long getNanoseconds() {
        return nanoseconds;
    }


    @Override
    public Common.IntervalMonthDayNano into() {
        return Common.IntervalMonthDayNano.newBuilder() //
                .setMonths(this.months) //
                .setDays(this.days) //
                .setNanoseconds(this.nanoseconds) //
                .build();
    }
}
