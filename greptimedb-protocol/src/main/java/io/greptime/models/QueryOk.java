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

import io.greptime.common.Streamable;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Contains the success value of query.
 *
 * @author jiachun.fjc
 */
public class QueryOk implements Streamable<Row> {

    private String     ql;
    private int        rowCount;
    private SelectRows rows;

    public String getQl() {
        return ql;
    }

    public int getRowCount() {
        return rowCount;
    }

    public SelectRows getRows() {
        return rows;
    }

    public Result<QueryOk, Err> mapToResult() {
        return Result.ok(this);
    }

    @Override
    public Stream<Row> stream() {
        Iterable<Row> iterable = () -> this.rows;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    @Override
    public String toString() {
        return "QueryOk{" + //
               "ql='" + ql + '\'' + //
               ", rowCount=" + rowCount + //
               '}';
    }

    public static QueryOk emptyOk() {
        return ok("", 0, SelectRows.empty());
    }

    public static QueryOk ok(String ql, int rowCount, SelectRows rows) {
        QueryOk ok = new QueryOk();
        ok.ql = ql;
        ok.rowCount = rowCount;
        ok.rows = rows;
        return ok;
    }
}
