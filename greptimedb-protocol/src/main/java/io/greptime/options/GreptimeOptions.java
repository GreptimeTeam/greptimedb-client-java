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
package io.greptime.options;

import io.greptime.common.Copiable;
import io.greptime.common.Endpoint;
import io.greptime.common.util.Ensures;
import io.greptime.limit.LimitedPolicy;
import io.greptime.models.AuthInfo;
import io.greptime.rpc.RpcOptions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * GreptimeDB client options.
 *
 * @author jiachun.fjc
 */
public class GreptimeOptions implements Copiable<GreptimeOptions> {
    private List<Endpoint> endpoints;
    private Executor asyncWritePool;
    private Executor asyncReadPool;
    private RpcOptions rpcOptions;
    private RouterOptions routerOptions;
    private WriteOptions writeOptions;
    private QueryOptions queryOptions;
    private AuthInfo authInfo;

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(List<Endpoint> endpoints) {
        this.endpoints = endpoints;
    }

    public Executor getAsyncWritePool() {
        return asyncWritePool;
    }

    public void setAsyncWritePool(Executor asyncWritePool) {
        this.asyncWritePool = asyncWritePool;
    }

    public Executor getAsyncReadPool() {
        return asyncReadPool;
    }

    public void setAsyncReadPool(Executor asyncReadPool) {
        this.asyncReadPool = asyncReadPool;
    }

    public RpcOptions getRpcOptions() {
        return rpcOptions;
    }

    public void setRpcOptions(RpcOptions rpcOptions) {
        this.rpcOptions = rpcOptions;
    }

    public RouterOptions getRouterOptions() {
        return routerOptions;
    }

    public void setRouterOptions(RouterOptions routerOptions) {
        this.routerOptions = routerOptions;
    }

    public WriteOptions getWriteOptions() {
        return writeOptions;
    }

    public void setWriteOptions(WriteOptions writeOptions) {
        this.writeOptions = writeOptions;
    }

    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    public void setQueryOptions(QueryOptions queryOptions) {
        this.queryOptions = queryOptions;
    }

    public AuthInfo getAuthInfo() {
        return authInfo;
    }

    public void setAuthInfo(AuthInfo authInfo) {
        this.authInfo = authInfo;
    }

    @Override
    public GreptimeOptions copy() {
        GreptimeOptions opts = new GreptimeOptions();
        opts.endpoints = new ArrayList<>(this.endpoints);
        opts.asyncWritePool = this.asyncWritePool;
        opts.asyncReadPool = this.asyncReadPool;
        opts.authInfo = this.authInfo;
        if (this.rpcOptions != null) {
            opts.rpcOptions = this.rpcOptions.copy();
        }
        if (this.routerOptions != null) {
            opts.routerOptions = this.routerOptions.copy();
        }
        if (this.writeOptions != null) {
            opts.writeOptions = this.writeOptions.copy();
        }
        if (this.queryOptions != null) {
            opts.queryOptions = this.queryOptions.copy();
        }
        return opts;
    }

    @Override
    public String toString() {
        return "GreptimeOptions{" + //
                "endpoints=" + endpoints + //
                ", asyncWritePool=" + asyncWritePool + //
                ", asyncReadPool=" + asyncReadPool + //
                ", rpcOptions=" + rpcOptions + //
                ", routerOptions=" + routerOptions + //
                ", writeOptions=" + writeOptions + //
                ", queryOptions=" + queryOptions + //
                ", authInfo=" + authInfo + //
                '}';
    }

    public static GreptimeOptions checkSelf(GreptimeOptions opts) {
        Ensures.ensureNonNull(opts, "null `opts (GreptimeOptions)`)`");
        Ensures.ensureNonNull(opts.getEndpoints(), "null `endpoints`");
        Ensures.ensure(!opts.getEndpoints().isEmpty(), "empty `endpoints`");
        Ensures.ensureNonNull(opts.getRpcOptions(), "null `rpcOptions`");
        Ensures.ensureNonNull(opts.getRouterOptions(), "null `routerOptions`");
        Ensures.ensureNonNull(opts.getWriteOptions(), "null `writeOptions`");
        Ensures.ensureNonNull(opts.getQueryOptions(), "null `queryOptions`");
        return opts;
    }

    public static Builder newBuilder(List<Endpoint> endpoints) {
        return new Builder(endpoints);
    }

    public static Builder newBuilder(Endpoint... endpoints) {
        return new Builder(Arrays.stream(endpoints).collect(Collectors.toList()));
    }

    public static Builder newBuilder(String... endpoints) {
        return new Builder(Arrays.stream(endpoints).map(Endpoint::parse).collect(Collectors.toList()));
    }

    public static final class Builder {
        private final List<Endpoint> endpoints = new ArrayList<>();

        // Asynchronous thread pool, which is used to handle various asynchronous tasks in the SDK.
        private Executor asyncWritePool;
        private Executor asyncReadPool;
        // Rpc options, in general the default configuration is fine.
        private RpcOptions rpcOptions = RpcOptions.newDefault();
        private int writeMaxRetries = 1;
        // Write flow limit: maximum number of data rows in-flight.
        private int maxInFlightWriteRows = 65536;
        private LimitedPolicy writeLimitedPolicy = LimitedPolicy.defaultWriteLimitedPolicy();
        private int defaultStreamMaxWritePointsPerSecond = 10 * 65536;
        // In some case of failure, a retry of the read is attempted.
        private int readMaxRetries = 1;
        // Refresh frequency of route tables. The background refreshes all route tables periodically. By default,
        // all route tables are refreshed every 30 seconds.
        private long routeTableRefreshPeriodSeconds = 30;
        // Authentication information
        private AuthInfo authInfo;

        public Builder(List<Endpoint> endpoints) {
            this.endpoints.addAll(endpoints);
        }

        /**
         * Asynchronous thread pool, which is used to handle various asynchronous
         * tasks in the SDK (You are using a purely asynchronous SDK). If you do not
         * set it, there will be a default implementation, which you can reconfigure
         * if the default implementation is not satisfied.
         * <p>
         * Note: We do not close it to free resources, as we view it as shared.
         *
         * @param asyncWritePool async thread pool for write
         * @param asyncReadPool  async thread pool for read
         * @return this builder
         */
        public Builder asyncPool(Executor asyncWritePool, Executor asyncReadPool) {
            this.asyncWritePool = asyncWritePool;
            this.asyncReadPool = asyncReadPool;
            return this;
        }

        /**
         * Sets the RPC options, in general, the default configuration is fine.
         *
         * @param rpcOptions the rpc options
         * @return this builder
         */
        public Builder rpcOptions(RpcOptions rpcOptions) {
            this.rpcOptions = rpcOptions;
            return this;
        }

        /**
         * In some case of failure, a retry of write can be attempted.
         *
         * @param maxRetries max retries times
         * @return this builder
         */
        public Builder writeMaxRetries(int maxRetries) {
            this.writeMaxRetries = maxRetries;
            return this;
        }

        /**
         * Write flow limit: maximum number of data rows in-flight.
         *
         * @param maxInFlightWriteRows max in-flight rows
         * @return this builder
         */
        public Builder maxInFlightWriteRows(int maxInFlightWriteRows) {
            this.maxInFlightWriteRows = maxInFlightWriteRows;
            return this;
        }

        /**
         * Set write limited policy.
         *
         * @param writeLimitedPolicy write limited policy
         * @return this builder
         */
        public Builder writeLimitedPolicy(LimitedPolicy writeLimitedPolicy) {
            this.writeLimitedPolicy = writeLimitedPolicy;
            return this;
        }

        /**
         * The default rate limit for stream writer.
         * @param defaultStreamMaxWritePointsPerSecond default max write points per second
         * @return this builder
         */
        public Builder defaultStreamMaxWritePointsPerSecond(int defaultStreamMaxWritePointsPerSecond) {
            this.defaultStreamMaxWritePointsPerSecond = defaultStreamMaxWritePointsPerSecond;
            return this;
        }

        /**
         * In some case of failure, a retry of read can be attempted.
         *
         * @param maxRetries max retries times
         * @return this builder
         */
        public Builder readMaxRetries(int maxRetries) {
            this.readMaxRetries = maxRetries;
            return this;
        }

        /**
         * Refresh frequency of route tables. The background refreshes all route tables
         * periodically. By default, all route tables are refreshed every 30 seconds.
         *
         * @param routeTableRefreshPeriodSeconds refresh period for route tables cache
         * @return this builder
         */
        public Builder routeTableRefreshPeriodSeconds(long routeTableRefreshPeriodSeconds) {
            this.routeTableRefreshPeriodSeconds = routeTableRefreshPeriodSeconds;
            return this;
        }

        /**
         * Set authentication information.
         *
         * @param authInfo the authentication information
         * @return this builder
         */
        public Builder authInfo(AuthInfo authInfo) {
            this.authInfo = authInfo;
            return this;
        }

        /**
         * A good start, happy coding.
         *
         * @return nice things
         */
        public GreptimeOptions build() {
            GreptimeOptions opts = new GreptimeOptions();
            opts.setEndpoints(this.endpoints);
            opts.setAsyncWritePool(this.asyncWritePool);
            opts.setAsyncReadPool(this.asyncReadPool);
            opts.setRpcOptions(this.rpcOptions);
            opts.setAuthInfo(this.authInfo);

            RouterOptions routerOpts = new RouterOptions();
            routerOpts.setEndpoints(this.endpoints);
            routerOpts.setRefreshPeriodSeconds(this.routeTableRefreshPeriodSeconds);
            opts.setRouterOptions(routerOpts);

            WriteOptions writeOpts = new WriteOptions();
            writeOpts.setAsyncPool(this.asyncWritePool);
            writeOpts.setMaxRetries(this.writeMaxRetries);
            writeOpts.setMaxInFlightWriteRows(this.maxInFlightWriteRows);
            writeOpts.setLimitedPolicy(this.writeLimitedPolicy);
            writeOpts.setDefaultStreamMaxWritePointsPerSecond(this.defaultStreamMaxWritePointsPerSecond);
            opts.setWriteOptions(writeOpts);

            QueryOptions queryOpts = new QueryOptions();
            queryOpts.setAsyncPool(this.asyncReadPool);
            queryOpts.setMaxRetries(this.readMaxRetries);
            opts.setQueryOptions(queryOpts);

            return GreptimeOptions.checkSelf(opts);
        }
    }
}
