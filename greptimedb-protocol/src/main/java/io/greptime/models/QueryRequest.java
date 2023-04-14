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
    private PromRangeQuery promRangeQuery;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<String> databaseName;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<AuthInfo> authInfo;

    public SelectExprType getExprType() {
        return exprType;
    }

    public String getQl() {
        return ql;
    }

    public PromRangeQuery getPromRangeQuery() {
        return promRangeQuery;
    }

    public void setAuthInfo(AuthInfo authInfo) {
        this.authInfo = Optional.of(authInfo);
    }

    @Override
    public String toString() {
        return "QueryRequest{" + //
                "exprType=" + exprType + //
                ", ql='" + ql + '\'' + //
                ", promRangeQuery=" + promRangeQuery + //
                ", databaseName=" + databaseName + //
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public Database.GreptimeRequest into() {
        Database.QueryRequest.Builder builder = Database.QueryRequest.newBuilder();

        Database.RequestHeader.Builder headerBuilder = Database.RequestHeader.newBuilder();
        this.authInfo.ifPresent(auth -> headerBuilder.setAuthorization(auth.into()));
        this.databaseName.ifPresent(headerBuilder::setDbname);

        switch (getExprType()) {
            case Sql:
                builder.setSql(getQl());
                break;
            case Promql:
                builder.setPromRangeQuery(getPromRangeQuery().into());
                break;
        }
        return Database.GreptimeRequest.newBuilder() //
                .setHeader(headerBuilder.build()) //
                .setQuery(builder.build()) //
                .build();
    }

    public static class Builder {
        private SelectExprType exprType;
        private String ql;
        private PromRangeQuery promRangeQuery;
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
         * The input parameters are similar to the `range_query` in Prometheus' HTTP API:
         *
         * @param promRangeQuery promql `range_query`
         * @return this builder
         */
        public Builder promQueryRange(PromRangeQuery promRangeQuery) {
            this.promRangeQuery = promRangeQuery;
            return this;
        }

        /**
         * Set name of the database the query runs on.
         *
         * @param databaseName the name
         * @return this builder
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
            switch (req.exprType) {
                case Sql:
                    req.ql = Ensures.ensureNonNull(this.ql, "null `ql`");
                    break;
                case Promql:
                    req.promRangeQuery = Ensures.ensureNonNull(this.promRangeQuery, "null `promQueryRange`");
                    break;
            }

            req.databaseName = Optional.ofNullable(this.databaseName);
            req.authInfo = Optional.empty();
            return req;
        }
    }
}
