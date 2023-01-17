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
package io.greptime.options;

import io.greptime.RouterClient;
import io.greptime.common.Copiable;
import java.util.concurrent.Executor;

/**
 * Write options.
 *
 * @author jiachun.fjc
 */
public class WriteOptions implements Copiable<WriteOptions> {
    private RouterClient routerClient;
    private Executor asyncPool;
    private int maxRetries = 1;

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

    @Override
    public WriteOptions copy() {
        WriteOptions opts = new WriteOptions();
        opts.routerClient = this.routerClient;
        opts.asyncPool = this.asyncPool;
        opts.maxRetries = this.maxRetries;
        return opts;
    }

    @Override
    public String toString() {
        return "WriteOptions{" + //
                ", routerClient=" + routerClient + //
                ", asyncPool=" + asyncPool + //
                ", maxRetries=" + maxRetries + //
                '}';
    }
}
