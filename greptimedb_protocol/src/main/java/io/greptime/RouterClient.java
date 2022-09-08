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
package io.greptime;

import io.greptime.common.Display;
import io.greptime.common.Endpoint;
import io.greptime.common.Lifecycle;
import io.greptime.common.util.Ensures;
import io.greptime.common.util.SharedScheduledPool;
import io.greptime.options.RouterOptions;
import io.greptime.rpc.Context;
import io.greptime.rpc.Observer;
import io.greptime.rpc.RpcClient;
import io.greptime.rpc.errors.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A route rpc client which cached the routing table information locally
 * and will auto refresh.
 *
 * @author jiachun.fjc
 */
public class RouterClient implements Lifecycle<RouterOptions>, Display {

    private static final Logger              LOG            = LoggerFactory.getLogger(RouterClient.class);

    private static final SharedScheduledPool REFRESHER_POOL = Util.getSharedScheduledPool("route_cache_refresher", 1);

    private ScheduledExecutorService         refresher;
    private RouterOptions                    opts;
    private RpcClient                        rpcClient;
    private InnerRouter                      inner;

    @Override
    public boolean init(RouterOptions opts) {
        this.opts = Ensures.ensureNonNull(opts, "Null RouterClient.opts").copy();
        this.rpcClient = this.opts.getRpcClient();

        List<Endpoint> endpoints = Ensures.ensureNonNull(this.opts.getEndpoints(), "Null endpoints");

        this.inner = new InnerRouter();
        this.inner.refreshLocal(endpoints);

        long refreshPeriod = this.opts.getRefreshPeriodSeconds();
        if (refreshPeriod > 0) {
            this.refresher = REFRESHER_POOL.getObject();
            this.refresher.scheduleWithFixedDelay(
                    () -> this.inner.refreshFromRemote(),
                    Util.randomInitialDelay(180), refreshPeriod, TimeUnit.SECONDS);

            LOG.info("Router cache refresher started.");
        }

        return true;
    }

    @Override
    public void shutdownGracefully() {
        if (this.rpcClient != null) {
            this.rpcClient.shutdownGracefully();
        }

        if (this.refresher != null) {
            REFRESHER_POOL.returnObject(this.refresher);
            this.refresher = null;
        }
    }

    public CompletableFuture<Endpoint> route() {
        return this.inner.routeFor(null);
    }

    public <Req, Resp> CompletableFuture<Resp> invoke(Endpoint endpoint, //
                                                      Req request, //
                                                      Context ctx) {
        return invoke(endpoint, request, ctx, -1 /* use default rpc timeout */);
    }

    public <Req, Resp> CompletableFuture<Resp> invoke(Endpoint endpoint, //
                                                      Req request, //
                                                      Context ctx, //
                                                      long timeoutMs) {
        CompletableFuture<Resp> future = new CompletableFuture<>();

        try {
            this.rpcClient.invokeAsync(endpoint, request, ctx, new Observer<Resp>() {

                @Override
                public void onNext(Resp value) {
                    future.complete(value);
                }

                @Override
                public void onError(Throwable err) {
                    future.completeExceptionally(err);
                }
            }, timeoutMs);

        } catch (RemotingException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @SuppressWarnings("unused")
    public <Req, Resp> void invokeServerStreaming(Endpoint endpoint, //
                                                  Req request, //
                                                  Context ctx, //
                                                  Observer<Resp> observer) {
        try {
            this.rpcClient.invokeServerStreaming(endpoint, request, ctx, observer);
        } catch (RemotingException e) {
            observer.onError(e);
        }
    }

    @SuppressWarnings("unused")
    public <Req, Resp> Observer<Req> invokeClientStreaming(Endpoint endpoint, //
                                                           Req defaultReqIns, //
                                                           Context ctx, //
                                                           Observer<Resp> respObserver) {
        try {
            return this.rpcClient.invokeClientStreaming(endpoint, defaultReqIns, ctx, respObserver);
        } catch (RemotingException e) {
            respObserver.onError(e);
            return new Observer.RejectedObserver<>(e);
        }
    }

    @Override
    public void display(Printer out) {
        out.println("--- RouterClient ---") //
            .print("opts=") //
            .println(this.opts);

        if (this.rpcClient != null) {
            out.println("");
            this.rpcClient.display(out);
        }
    }

    @Override
    public String toString() {
        return "RouterClient{" + //
               "refresher=" + refresher + //
               ", opts=" + opts + //
               ", rpcClient=" + rpcClient + //
               '}';
    }

    /**
     * Request to a `frontend` server, which needs to return all members(frontend server),
     * or it can return only one domain address, it is also possible to return no address
     * at all, depending on the specific policy:
     * <p>
     * 1. The client configures only one static address and sends requests to that address
     * permanently. In other words: the router doesn't do anything, and this policy can't
     * dynamically switch addresses.
     * <p>
     * 2. The client configures some addresses (the address list of the frontend server),
     * the client send request using a rr or random strategy, and frontend server needs to
     * be able to return  the member list for the purpose of frontend server members change.
     */
    private static class InnerRouter implements Router<Void, Endpoint> {

        private final AtomicReference<List<Endpoint>> endpointsRef = new AtomicReference<>();

        public void refreshFromRemote() {
            // TODO
        }

        void refreshLocal(List<Endpoint> input) {
            this.endpointsRef.set(input);
        }

        @Override
        public CompletableFuture<Endpoint> routeFor(Void request) {
            List<Endpoint> endpoints = this.endpointsRef.get();
            ThreadLocalRandom random = ThreadLocalRandom.current();
            int i = random.nextInt(0, endpoints.size());
            return Util.completedCf(endpoints.get(i));
        }
    }
}
