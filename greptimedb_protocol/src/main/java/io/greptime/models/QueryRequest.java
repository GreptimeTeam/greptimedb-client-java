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

/**
 * The query request condition.
 *
 * @author jiachun.fjc
 */
public class QueryRequest {
    private SelectExpr expr;
    private String     ql;

    public SelectExpr getExpr() {
        return expr;
    }

    public String getQl() {
        return ql;
    }

    @Override
    public String toString() {
        return "QueryRequest{" + //
               "expr=" + expr + //
               ", ql='" + ql + '\'' + //
               '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private SelectExpr expr;
        private String     ql;

        /**
         * Sets select expression type, such as sql, promql, etc.
         *
         * @param expr expr type
         * @return this builder
         */
        public Builder expr(SelectExpr expr) {
            this.expr = expr;
            return this;
        }

        /**
         * Query language to.
         *
         * @param ql the query language
         * @return this builder
         */
        public Builder ql(String ql) {
            this.ql = ql;
            return this;
        }

        /**
         * Query language to, using the specified format string and arguments.
         *
         * @param fmtQl format ql string
         * @param args  arguments referenced by the format specifiers in the format
         *              QL string.  If there are more arguments than format specifiers,
         *              the extra arguments are ignored.  The number of arguments is
         *               variable and may be zero.
         * @return this builder
         */
        public Builder ql(String fmtQl, Object... args) {
            this.ql = String.format(fmtQl, args);
            return this;
        }

        public QueryRequest build() {
            QueryRequest req = new QueryRequest();
            req.expr = this.expr;
            req.ql = this.ql;
            return req;
        }
    }
}
