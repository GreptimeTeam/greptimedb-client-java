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
import io.greptime.v1.Columns;
import io.greptime.v1.Common;
import org.junit.Assert;
import org.junit.Test;
import java.util.BitSet;

/**
 *
 * @author jiachun.fjc
 */
public class ColumnHelperTest {

    @Test
    public void testGetValueSomeNull() {
        BitSet nullMask = new BitSet(5);
        nullMask.set(1, true);
        nullMask.set(3, true);
        Columns.Column column = Columns.Column.newBuilder().setColumnName("test_column") //
                .setSemanticType(Common.SemanticType.FIELD) //
                .setValues(Columns.Column.Values.newBuilder() //
                        .addI32Values(1) //
                        .addI32Values(3) //
                        .addI32Values(5) //
                        .build()) //
                .setDatatype(Common.ColumnDataType.INT32) //
                .setNullMask(ByteStringHelper.wrap(nullMask.toByteArray())) //
                .build();

        Object v = ColumnHelper.getValue(column, 0, ColumnHelper.getNullMaskBits(column));
        Assert.assertEquals(1, v);
        v = ColumnHelper.getValue(column, 1, ColumnHelper.getNullMaskBits(column));
        Assert.assertNull(v);
        v = ColumnHelper.getValue(column, 2, ColumnHelper.getNullMaskBits(column));
        Assert.assertEquals(3, v);
        v = ColumnHelper.getValue(column, 3, ColumnHelper.getNullMaskBits(column));
        Assert.assertNull(v);
        v = ColumnHelper.getValue(column, 4, ColumnHelper.getNullMaskBits(column));
        Assert.assertEquals(5, v);
    }

    @Test
    public void testGetValueNonNull() {
        Columns.Column column = Columns.Column.newBuilder().setColumnName("test_column") //
                .setSemanticType(Common.SemanticType.FIELD) //
                .setValues(Columns.Column.Values.newBuilder() //
                        .addI32Values(1) //
                        .addI32Values(2) //
                        .addI32Values(3) //
                        .build()) //
                .setDatatype(Common.ColumnDataType.INT32) //
                .build();

        Object v = ColumnHelper.getValue(column, 0, ColumnHelper.getNullMaskBits(column));
        Assert.assertEquals(1, v);
        v = ColumnHelper.getValue(column, 1, ColumnHelper.getNullMaskBits(column));
        Assert.assertEquals(2, v);
        v = ColumnHelper.getValue(column, 2, ColumnHelper.getNullMaskBits(column));
        Assert.assertEquals(3, v);
    }
}
