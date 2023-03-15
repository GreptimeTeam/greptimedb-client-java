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
        TableSchema schema = TableSchema.newBuilder(TableName.with("", "test_table")) //
                .columnNames("col1", "col2", "col3") //
                .semanticTypes(SemanticType.Tag, SemanticType.Tag, SemanticType.Field) //
                .dataTypes(ColumnDataType.String, ColumnDataType.String, ColumnDataType.Int32) //
                .build();

        WriteRows rows = WriteRows.newBuilder(schema).build();
        rows.insert("1", "11", 111) //
                .insert("2", "22", 222) //
                .insert("3", "33", 333) //
                .finish();

        Assert.assertEquals(3, rows.rowCount());
        Assert.assertEquals(111, rows.columns().get(2).getValues().getI32Values(0));
        Assert.assertEquals(222, rows.columns().get(2).getValues().getI32Values(1));
        Assert.assertEquals(333, rows.columns().get(2).getValues().getI32Values(2));
    }

    @Test
    public void testWriteRowsSomeNull() {
        TableSchema schema = TableSchema.newBuilder(TableName.with("", "test_table")) //
                .columnNames("col1", "col2", "col3") //
                .semanticTypes(SemanticType.Tag, SemanticType.Tag, SemanticType.Field) //
                .dataTypes(ColumnDataType.String, ColumnDataType.String, ColumnDataType.Int32) //
                .build();

        WriteRows rows = WriteRows.newBuilder(schema).build();
        rows.insert("1", "11", 111) //
                .insert("2", null, 222) //
                .insert("3", "33", null) //
                .finish();

        Assert.assertEquals(3, rows.rowCount());
        Assert.assertEquals(111, rows.columns().get(2).getValues().getI32Values(0));
        Assert.assertEquals(222, rows.columns().get(2).getValues().getI32Values(1));
        Assert.assertEquals("33", rows.columns().get(1).getValues().getStringValues(1));
        Assert.assertTrue(rows.columns().get(0).getNullMask().isEmpty());
        Assert.assertTrue(bitSet(rows.columns().get(1).getNullMask()).get(1));
        Assert.assertTrue(bitSet(rows.columns().get(2).getNullMask()).get(2));
    }

    private BitSet bitSet(ByteString byteString) {
        return BitSet.valueOf(ByteStringHelper.sealByteArray(byteString));
    }
}
