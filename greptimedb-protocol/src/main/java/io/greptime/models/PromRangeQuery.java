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
import io.greptime.common.util.Strings;
import io.greptime.v1.Database;

/**
 * Similar to the range_query in Prometheus' HTTP API:
 * <p>
 *  query=<string>: Prometheus expression query string
 *  start=<rfc3339 | unix_timestamp>: start timestamp, inclusive
 *  end=<rfc3339 | unix_timestamp>: end timestamp, inclusive
 *  step=<duration | float>: query resolution step width in duration format or float number of seconds
 *
 * @author jiachun.fjc
 */
public class PromRangeQuery implements Into<Database.PromRangeQuery> {

    /** Prometheus expression query string. */
    private String query;
    /** start timestamp, inclusive. */
    private String start;
    /** end timestamp, inclusive. */
    private String end;
    /** query resolution step width in duration format or float number of seconds. */
    private String step;

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public String getStep() {
        return step;
    }

    public void setStep(String step) {
        this.step = step;
    }

    @Override
    public Database.PromRangeQuery into() {
        return Database.PromRangeQuery.newBuilder() //
                .setQuery(getQuery()) //
                .setStart(getStart()) //
                .setEnd(getEnd()) //
                .setStep(getStep()) //
                .build();
    }

    public static Builder newBuildr() {
        return new Builder();
    }

    public static class Builder {
        /** Prometheus expression query string. */
        private String query;
        /** start timestamp, inclusive. */
        private String start;
        /** end timestamp, inclusive. */
        private String end;
        /** query resolution step width in duration format or float number of seconds. */
        private String step;

        /**
         * query=<string>
         *
         * @param query prometheus expression query string
         * @return this builder
         */
        public Builder query(String query) {
            this.query = query;
            return this;
        }

        /**
         * start=<rfc3339 | unix_timestamp>
         * <p>
         * rfc3339
         *  2015-07-01T20:11:00Z (default to seconds resolution)
         *  2015-07-01T20:11:00.781Z (with milliseconds resolution)
         *  2015-07-02T04:11:00+08:00 (with timezone offset)
         * <p>
         * unix timestamp
         *  1435781460 (default to seconds resolution)
         *  1435781460.781 (with milliseconds resolution)
         *
         * @param start start timestamp, inclusive
         * @return this builder
         */
        public Builder start(String start) {
            this.start = start;
            return this;
        }

        /**
         * end=<rfc3339 | unix_timestamp>
         * <p>
         * rfc3339
         *  2015-07-01T20:11:00Z (default to seconds resolution)
         *  2015-07-01T20:11:00.781Z (with milliseconds resolution)
         *  2015-07-02T04:11:00+08:00 (with timezone offset)
         * <p>
         * unix timestamp
         *  1435781460 (default to seconds resolution)
         *  1435781460.781 (with milliseconds resolution)
         *
         * @param end end timestamp, inclusive
         * @return this builder
         */
        public Builder end(String end) {
            this.end = end;
            return this;
        }

        /**
         * step=<duration | float>
         * <p>
         * duration
         *  1h (1 hour)
         *  5d1m (5 days and 1 minute)
         *  2 (2 seconds)
         *  2s (also 2 seconds)
         *
         * @param step query resolution step width in duration format or float number of seconds
         * @return this builder
         */
        public Builder step(String step) {
            this.step = step;
            return this;
        }

        public PromRangeQuery build() {
            PromRangeQuery query = new PromRangeQuery();

            Ensures.ensure(Strings.isNotBlank(this.query), "blank `query`");
            Ensures.ensure(Strings.isNotBlank(this.start), "blank `start`");
            Ensures.ensure(Strings.isNotBlank(this.end), "blank `end`");
            Ensures.ensure(Strings.isNotBlank(this.step), "blank `step`");

            query.query = this.query;
            query.start = this.start;
            query.end = this.end;
            query.step = this.step;
            return query;
        }
    }
}
