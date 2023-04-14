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

import io.greptime.RouterClient;
import io.greptime.common.Copiable;
import io.greptime.models.AuthInfo;
import java.util.concurrent.Executor;

/**
 * Query options.
 *
 * @author jiachun.fjc
 */
public class QueryOptions implements Copiable<QueryOptions> {
    private RouterClient routerClient;
    private Executor asyncPool;
    private int maxRetries = 1;
    private AuthInfo authInfo;

    public RouterClient getRouterClient() {
        return routerClient;
    }

    public void setRouterClient(RouterClient routerClient) {
        this.routerClient = routerClient;
    }

    public Executor getAsyncPool() {
        return asyncPool;
    }

    public void setAsyncPool(Executor asyncPool) {
        this.asyncPool = asyncPool;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public AuthInfo getAuthInfo() {
        return authInfo;
    }

    public void setAuthInfo(AuthInfo authInfo) {
        this.authInfo = authInfo;
    }

    @Override
    public QueryOptions copy() {
        QueryOptions opts = new QueryOptions();
        opts.routerClient = this.routerClient;
        opts.asyncPool = this.asyncPool;
        opts.maxRetries = this.maxRetries;
        opts.authInfo = this.authInfo;
        return opts;
    }

    @Override
    public String toString() {
        return "QueryOptions{" + //
                "routerClient=" + routerClient + //
                ", asyncPool=" + asyncPool + //
                ", maxRetries=" + maxRetries + //
                ", authInfo=" + authInfo + //
                '}';
    }
}
