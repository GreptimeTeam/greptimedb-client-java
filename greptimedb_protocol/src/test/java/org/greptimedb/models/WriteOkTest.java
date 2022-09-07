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
package org.greptimedb.models;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class WriteOkTest {

    @Test
    public void testCombine() {
        WriteOk writeOk = WriteOk.ok(200, 2, "test1");
        writeOk = writeOk.combine(WriteOk.ok(100, 0, "test1"));

        Assert.assertEquals(300, writeOk.getSuccess());
        Assert.assertEquals(2, writeOk.getFailed());
        Assert.assertEquals("test1", writeOk.getTableName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCombineMultiTable() {
        WriteOk writeOk = WriteOk.ok(200, 2, "test1");
        writeOk.combine(WriteOk.ok(100, 0, "test2"));
    }

    @Test
    public void testCombineWithEmptyTableName() {
        WriteOk writeOk = WriteOk.ok(200, 2, null);
        writeOk = writeOk.combine(WriteOk.ok(100, 0, null));

        Assert.assertEquals(300, writeOk.getSuccess());
        Assert.assertEquals(2, writeOk.getFailed());
        Assert.assertNull(writeOk.getTableName());

        writeOk.combine(WriteOk.ok(100, 0, null));

        Assert.assertEquals(400, writeOk.getSuccess());
        Assert.assertEquals(2, writeOk.getFailed());
        Assert.assertNull(writeOk.getTableName());
    }

    @Test
    public void testMapToResult() {
        WriteOk writeOk = WriteOk.ok(200, 2, "test_table");
        Result<WriteOk, Err> res = writeOk.mapToResult();

        Assert.assertTrue(res.isOk());
    }

    @Test
    public void testEmptyOk() {
        WriteOk empty = WriteOk.emptyOk();

        Assert.assertEquals(0, empty.getSuccess());
        Assert.assertEquals(0, empty.getFailed());
        Assert.assertTrue(empty.mapToResult().isOk());
    }
}
