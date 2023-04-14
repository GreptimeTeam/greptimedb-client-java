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

import io.greptime.models.SelectRows.DefaultSelectRows;
import io.greptime.rpc.Context;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Assert;
import org.junit.Test;
import java.util.Arrays;

/**
 * @author jiachun.fjc
 */
public class SelectRowsTest {

    @Test
    public void testSelectRowsIterator() {
        DefaultSelectRows rows = new DefaultSelectRows(Context.newDefault(), null, null);

        rows.consume(genRecordbatch());

        Row r = rows.next();
        Assert.assertEquals("test_column1", r.values().get(0).name());
        Assert.assertEquals(ColumnDataType.Int32, r.values().get(0).dataType());
        Assert.assertEquals(1, r.values().get(0).value());
        Assert.assertEquals("test_column2", r.values().get(1).name());
        Assert.assertEquals(ColumnDataType.Int32, r.values().get(1).dataType());
        Assert.assertEquals(1, r.values().get(1).value());

        r = rows.next();
        Assert.assertNull(r.values().get(0).value());
        Assert.assertEquals(2, r.values().get(1).value());

        r = rows.next();
        Assert.assertEquals(3, r.values().get(0).value());
        Assert.assertEquals(3, r.values().get(1).value());

        r = rows.next();
        Assert.assertNull(r.values().get(0).value());
        Assert.assertEquals(4, r.values().get(1).value());

        r = rows.next();
        Assert.assertEquals(5, r.values().get(0).value());
        Assert.assertEquals(5, r.values().get(1).value());
    }

    private VectorSchemaRoot genRecordbatch() {
        RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

        Field field1 = new Field("test_column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("test_column2", FieldType.nullable(new ArrowType.Int(32, true)), null);

        IntVector column1 = new IntVector("test_column1", allocator);
        column1.setSafe(0, 1);
        column1.setSafe(2, 3);
        column1.setSafe(4, 5);

        IntVector column2 = new IntVector("test_column2", allocator);
        for (int i = 0; i < 5; i++) {
            column2.setSafe(i, i + 1);
        }

        return new VectorSchemaRoot(Arrays.asList(field1, field2), Arrays.asList(column1, column2), 5);
    }
}
