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

import io.greptime.common.Endpoint;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class ErrTest {

    @Test
    public void testWriteErr() {
        Err err = Err.writeErr(300, new IllegalStateException("test_err"), Endpoint.of("127.0.0.1", 8081), null);

        Assert.assertFalse(err.mapToResult().isOk());
        Assert.assertEquals("test_err", err.getError().getMessage());
        Assert.assertEquals("127.0.0.1:8081", err.getErrTo().toString());
        Assert.assertNull(err.getRowsFailed());
    }

    @Test
    public void testQueryErr() {
        IllegalStateException error = new IllegalStateException("test_err");
        Err err = Err.queryErr(300, error, Endpoint.of("127.0.0.1", 8081), "select * from test");

        Assert.assertFalse(err.mapToResult().isOk());
        Assert.assertEquals("test_err", err.getError().getMessage());
        Assert.assertEquals("127.0.0.1:8081", err.getErrTo().toString());
        Assert.assertEquals(err.getFailedQl(), "select * from test");
    }
}
