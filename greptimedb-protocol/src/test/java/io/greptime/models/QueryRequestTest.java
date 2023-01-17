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

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class QueryRequestTest {

    @Test(expected = NullPointerException.class)
    public void testBuildRequestWithNullExprType() {
        QueryRequest.newBuilder() //
                .ql("select * from test_table") //
                .build();
    }

    @Test
    public void testBuildRequestWithArgsQl() {
        QueryRequest req = QueryRequest.newBuilder() //
                .exprType(SelectExprType.Sql) //
                .ql("select %s from %s", "a", "test_table") //
                .build();

        Assert.assertEquals(SelectExprType.Sql, req.getExprType());
        Assert.assertEquals("select a from test_table", req.getQl());
    }
}
