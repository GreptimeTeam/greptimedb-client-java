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

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.BitSet;

/**
 * @author jiachun.fjc
 */
public class WriteRowsTest {

    @Test
    public void testWriteRowsNonNull() {
        WriteRows rows = WriteRows.newBuilder("test_table") //
            .columnNames("col1", "col2", "col3") //
            .semanticTypes(SemanticType.Tag, SemanticType.Tag, SemanticType.Field) //
            .dataTypes(ColumnDataType.String, ColumnDataType.String, ColumnDataType.Int32) //
            .build();

        rows.insert("1", "11", 111) //
            .insert("2", "22", 222) //
            .insert("3", "33", 333) //
            .finish();

        Assert.assertEquals(3, rows.getRowCount());
        Assert.assertEquals(111, rows.getColumns().get(2).getValues().getI32Values(0));
        Assert.assertEquals(222, rows.getColumns().get(2).getValues().getI32Values(1));
        Assert.assertEquals(333, rows.getColumns().get(2).getValues().getI32Values(2));
    }

    @Test
    public void testWriteRowsSomeNull() {
        WriteRows rows = WriteRows.newBuilder("test_table") //
            .columnNames("col1", "col2", "col3") //
            .semanticTypes(SemanticType.Tag, SemanticType.Tag, SemanticType.Field) //
            .dataTypes(ColumnDataType.String, ColumnDataType.String, ColumnDataType.Int32) //
            .build();

        rows.insert("1", "11", 111) //
            .insert("2", null, 222) //
            .insert("3", "33", null) //
            .finish();

        Assert.assertEquals(3, rows.getRowCount());
        Assert.assertEquals(111, rows.getColumns().get(2).getValues().getI32Values(0));
        Assert.assertEquals(222, rows.getColumns().get(2).getValues().getI32Values(1));
        Assert.assertEquals("33", rows.getColumns().get(1).getValues().getStringValues(1));
        Assert.assertTrue(rows.getColumns().get(0).getNullMask().isEmpty());
        Assert.assertTrue(bitSet(rows.getColumns().get(1).getNullMask()).get(1));
        Assert.assertTrue(bitSet(rows.getColumns().get(2).getNullMask()).get(2));
    }

    private BitSet bitSet(ByteString byteString) {
        return BitSet.valueOf(ByteStringHelper.sealByteArray(byteString));
    }
}
