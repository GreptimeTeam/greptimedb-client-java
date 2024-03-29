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

import io.greptime.common.Streamable;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Contains the success value of query.
 *
 * @author jiachun.fjc
 */
public class QueryOk implements Streamable<Row> {

    private String ql;
    private SelectRows rows;

    /**
     * Returns the QL.
     */
    public String getQl() {
        return ql;
    }

    /**
     * Returns the data of rows.
     */
    public SelectRows getRows() {
        return rows;
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
                '}';
    }

    /**
     * Creates a new {@link QueryOk}.
     *
     * @param ql the QL
     * @param rows the selected rows
     * @return a new {@link QueryOk}
     */
    public static QueryOk ok(String ql, SelectRows rows) {
        QueryOk ok = new QueryOk();
        ok.ql = ql;
        ok.rows = rows;
        return ok;
    }
}
