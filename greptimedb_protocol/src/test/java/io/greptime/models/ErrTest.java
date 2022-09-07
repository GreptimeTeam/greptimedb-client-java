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

import io.greptime.common.Endpoint;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author jiachun.fjc
 */
public class ErrTest {

    @Test
    public void testCombineWriteOk() {
        WriteOk subOK1 = WriteOk.ok(2, 0, "test_table");
        Err err = Err.writeErr(300, "test_err", Endpoint.of("127.0.0.1", 8081), null);

        Assert.assertEquals("127.0.0.1:8081", err.getErrTo().toString());
        Assert.assertNull(err.getRowsFailed());

        err = err.combine(subOK1);

        Assert.assertEquals(2, err.getSubOk().getSuccess());

        WriteOk subOK2 = WriteOk.ok(3, 0, "test_table");
        err = err.combine(subOK2);

        Assert.assertEquals(5, err.getSubOk().getSuccess());
    }

    @Test
    public void testCombineErr() {
        Err err = Err.writeErr(300, "test_err", Endpoint.of("127.0.0.1", 8081), null);
        err.combine(Err.writeErr(300, "test_err2", Endpoint.of("127.0.0.1", 8081), null));

        List<Err> list = err.stream().collect(Collectors.toList());

        Assert.assertEquals(2, list.size());
        Assert.assertEquals("test_err", list.get(0).getError());
        Assert.assertEquals("test_err2", list.get(1).getError());
    }
}
