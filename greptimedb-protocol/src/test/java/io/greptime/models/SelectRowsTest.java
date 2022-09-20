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
import io.greptime.v1.Columns;
import io.greptime.v1.codec.Select;
import org.junit.Assert;
import org.junit.Test;

import java.util.BitSet;
import java.util.List;

/**
 * @author jiachun.fjc
 */
public class SelectRowsTest {

    @Test
    public void testSelectRowsCollect() {
        SelectRows rows = SelectRows.from(genSelectResult());
        List<Row> res = rows.collect();
        Assert.assertEquals(5, res.size());
        for (Row r : res) {
            Assert.assertEquals(2, r.values().size());
        }
    }

    @Test
    public void testSelectRowsIterator() {
        SelectRows rows = SelectRows.from(genSelectResult());
        Assert.assertEquals(5, rows.rowCount());
        Assert.assertTrue(rows.hasNext());
        Row r = rows.next();
        Assert.assertEquals("test_column1", r.values().get(0).name());
        Assert.assertEquals(SemanticType.Field, r.values().get(0).semanticType());
        Assert.assertEquals(ColumnDataType.Int32, r.values().get(0).dataType());
        Assert.assertEquals(1, r.values().get(0).value());
        Assert.assertEquals("test_column2", r.values().get(1).name());
        Assert.assertEquals(SemanticType.Field, r.values().get(1).semanticType());
        Assert.assertEquals(ColumnDataType.Int32, r.values().get(1).dataType());
        Assert.assertEquals(1, r.values().get(1).value());

        Assert.assertTrue(rows.hasNext());
        r = rows.next();
        Assert.assertNull(r.values().get(0).value());
        Assert.assertEquals(2, r.values().get(1).value());

        Assert.assertTrue(rows.hasNext());
        r = rows.next();
        Assert.assertEquals(3, r.values().get(0).value());
        Assert.assertEquals(3, r.values().get(1).value());

        Assert.assertTrue(rows.hasNext());
        r = rows.next();
        Assert.assertNull(r.values().get(0).value());
        Assert.assertEquals(4, r.values().get(1).value());

        Assert.assertTrue(rows.hasNext());
        r = rows.next();
        Assert.assertEquals(5, r.values().get(0).value());
        Assert.assertEquals(5, r.values().get(1).value());

        Assert.assertFalse(rows.hasNext());
    }

    private Select.SelectResult genSelectResult() {
        BitSet nullMask = new BitSet(5);
        nullMask.set(1, true);
        nullMask.set(3, true);
        Columns.Column column1 = Columns.Column.newBuilder().setColumnName("test_column1") //
            .setSemanticType(Columns.Column.SemanticType.FIELD) //
            .setValues(Columns.Column.Values.newBuilder() //
                .addI32Values(1) //
                .addI32Values(3) //
                .addI32Values(5) //
                .build()) //
            .setDatatype(Columns.ColumnDataType.INT32) //
            .setNullMask(ByteStringHelper.wrap(nullMask.toByteArray())) //
            .build();

        Columns.Column column2 = Columns.Column.newBuilder().setColumnName("test_column2") //
            .setSemanticType(Columns.Column.SemanticType.FIELD) //
            .setValues(Columns.Column.Values.newBuilder() //
                .addI32Values(1) //
                .addI32Values(2) //
                .addI32Values(3) //
                .addI32Values(4) //
                .addI32Values(5) //
                .build()) //
            .setDatatype(Columns.ColumnDataType.INT32) //
            .build();

        return Select.SelectResult.newBuilder().addColumns(column1) //
            .addColumns(column2) //
            .setRowCount(5) //
            .build();
    }
}
