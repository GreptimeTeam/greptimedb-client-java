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
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Random;
import java.util.TimeZone;

/**
 * @author jiachun.fjc
 */
public class UtilTest {

    @Test
    public void testGetLongValue() {
        Assert.assertEquals(1L, Util.getLongValue(1));
        Assert.assertEquals(1L, Util.getLongValue(1L));
        Assert.assertEquals(1L, Util.getLongValue(1.0));
        Assert.assertEquals(1L, Util.getLongValue(1.0f));
        Assert.assertEquals(1L, Util.getLongValue(BigInteger.valueOf(1)));
        Assert.assertEquals(1L, Util.getLongValue(BigDecimal.valueOf(1)));
    }

    @Test
    public void testGetDateValue() {
        Calendar cal = Calendar.getInstance();
        TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT");
        cal.setTimeZone(gmtTimeZone);
        cal.set(1970, Calendar.JANUARY, 2);
        Assert.assertEquals(1, Util.getDateValue(cal.getTime()));
        Assert.assertEquals(1, Util.getDateValue(Instant.ofEpochSecond(86400)));
        Assert.assertEquals(1, Util.getDateValue(LocalDate.ofEpochDay(1)));
        Assert.assertEquals(1, Util.getDateValue(1));
    }

    @Test
    public void testGetDateTimeValue() {
        Calendar cal = Calendar.getInstance();
        TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT");
        cal.setTimeZone(gmtTimeZone);
        cal.set(1970, Calendar.JANUARY, 2, 0, 0, 0);
        Assert.assertEquals(86400, Util.getDateTimeValue(cal.getTime()));
        Assert.assertEquals(86400, Util.getDateTimeValue(Instant.ofEpochSecond(86400)));
        Assert.assertEquals(86400, Util.getDateTimeValue(86400));
    }

    @Test
    public void testGetDecimal128Value() {
        final int precision = 38;
        final int scale = 9;

        Common.DecimalTypeExtension decimalTypeExtension = Common.DecimalTypeExtension.newBuilder() //
                .setPrecision(precision) //
                .setScale(scale) //
                .build();
        Common.ColumnDataTypeExtension dataTypeExtension = Common.ColumnDataTypeExtension.newBuilder() //
                .setDecimalType(decimalTypeExtension) //
                .build();

        for (int i = 0; i < 1000; i++) {
            BigInteger bigInt = BigInteger.valueOf(new Random().nextLong()).shiftLeft(64);
            bigInt = bigInt.add(BigInteger.valueOf(new Random().nextLong()));
            BigDecimal value = new BigDecimal(bigInt, scale);
            Common.Decimal128 result = Util.getDecimal128Value(dataTypeExtension, value);

            long lo = result.getLo();
            BigInteger loValue = BigInteger.valueOf(lo & Long.MAX_VALUE);
            if (lo < 0) {
                loValue = loValue.add(BigInteger.valueOf(1).shiftLeft(63));
            }

            BigInteger unscaledValue = BigInteger.valueOf(result.getHi());
            unscaledValue = unscaledValue.shiftLeft(64);
            unscaledValue = unscaledValue.add(loValue);

            BigDecimal value2 = new BigDecimal(unscaledValue, scale);

            Assert.assertEquals(value, value2);
        }
    }
}
