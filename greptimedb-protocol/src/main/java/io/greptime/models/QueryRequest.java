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

import io.greptime.common.Into;
import io.greptime.common.util.Ensures;
import io.greptime.v1.Database;
import java.util.Optional;

/**
 * The query request condition.
 *
 * @author jiachun.fjc
 */
public class QueryRequest implements Into<Database.GreptimeRequest> {
    private SelectExprType exprType;
    private String ql;
    private Optional<String> databaseName;
    private Optional<AuthInfo> authInfo;

    public SelectExprType getExprType() {
        return exprType;
    }

    public String getQl() {
        return ql;
    }

    public void setAuthInfo(AuthInfo authInfo) {
        this.authInfo = Optional.of(authInfo);
    }

    @Override
    public String toString() {
        return "QueryRequest{" + //
                "exprType=" + exprType + //
                ", ql='" + ql + '\'' + //
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public Database.GreptimeRequest into() {
        Database.QueryRequest.Builder builder = Database.QueryRequest.newBuilder();

        Database.RequestHeader.Builder header_builder = Database.RequestHeader.newBuilder();
        if (authInfo.isPresent()) {
            Database.AuthHeader authHeader = authInfo.get().intoAuthHeader();
            header_builder.setAuthorization(authHeader);
        }

        if (databaseName.isPresent()) {
            header_builder.setDbname(databaseName.get());
        }

        switch (getExprType()) {
            case Sql:
                builder.setSql(getQl());
                break;
            case Promql:
                throw new UnsupportedOperationException("Promql unsupported yet!");
        }
        return Database.GreptimeRequest.newBuilder().setHeader(header_builder.build()).setQuery(builder.build())
                .build();
    }

    public static class Builder {
        private SelectExprType exprType;
        private String ql;
        private String databaseName;

        /**
         * Sets select expression type, such as sql, promql, etc.
         *
         * @param exprType expr type
         * @return this builder
         */
        public Builder exprType(SelectExprType exprType) {
            this.exprType = exprType;
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
         * Set name of the database the query runs on.
         *
         * @param databaseName the name
         * @return builder self
         */
        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Query language to, using the specified format string and arguments.
         *
         * @param fmtQl format ql string
         * @param args  arguments referenced by the format specifiers in the format
         *              QL string.  If there are more arguments than format specifiers,
         *              the extra arguments are ignored.  The number of arguments is
         *              variable and may be zero.
         * @return this builder
         */
        public Builder ql(String fmtQl, Object... args) {
            this.ql = String.format(fmtQl, args);
            return this;
        }

        public QueryRequest build() {
            QueryRequest req = new QueryRequest();
            req.exprType = Ensures.ensureNonNull(this.exprType, "null `exprType`");
            req.ql = Ensures.ensureNonNull(this.ql, "null `ql`");
            req.databaseName = Optional.ofNullable(this.databaseName);
            req.authInfo = Optional.empty();
            return req;
        }
    }

}
