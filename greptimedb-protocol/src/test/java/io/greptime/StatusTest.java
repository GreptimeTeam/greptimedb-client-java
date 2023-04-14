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
package io.greptime;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class StatusTest {

    @Test
    public void testParseStatus() {
        int code = Status.Success.getStatusCode();
        Status status = Status.parse(code);
        Assert.assertEquals(Status.Success, status);
    }

    @Test
    public void testParseInvalidStatus() {
        Status status = Status.parse(9999);
        Assert.assertNull(status);
    }
}
