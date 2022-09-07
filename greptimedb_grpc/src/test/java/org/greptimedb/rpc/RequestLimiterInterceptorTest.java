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
package org.greptimedb.rpc;

import com.netflix.concurrency.limits.Limiter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import org.greptimedb.common.util.MetricsUtil;
import org.greptimedb.rpc.interceptors.ClientRequestLimitInterceptor;
import org.greptimedb.rpc.limit.LimitMetricRegistry;
import org.greptimedb.rpc.limit.RequestLimitCtx;
import org.greptimedb.rpc.limit.RequestLimiterBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Refer to `concurrency-limit-grpc's test`
 *
 * @author jiachun.fjc
 */
public class RequestLimiterInterceptorTest {

    private static final MethodDescriptor<String, String> METHOD_DESCRIPTOR;

    static {
        METHOD_DESCRIPTOR = MethodDescriptor.<String, String> newBuilder() //
            .setType(MethodDescriptor.MethodType.UNARY) //
            .setFullMethodName("service/method") //
            .setRequestMarshaller(StringMarshaller.INSTANCE) //
            .setResponseMarshaller(StringMarshaller.INSTANCE) //
            .build();
    }

    @Ignore("Not a unit test, just for simulate the operation of the limiter")
    @Test
    public void simulation() throws IOException {
        Semaphore sem = new Semaphore(10, true);
        Server server = NettyServerBuilder.forPort(0)
                .addService(ServerServiceDefinition.builder("service")
                        .addMethod(METHOD_DESCRIPTOR, ServerCalls.asyncUnaryCall((req, observer) -> {
                            try {
                                sem.acquire();
                                TimeUnit.MILLISECONDS.sleep(100);
                            } catch (InterruptedException ignored) {
                            } finally {
                                sem.release();
                            }

                            observer.onNext("response");
                            observer.onCompleted();
                        }))
                        .build())
                .build()
                .start();

        Limiter<RequestLimitCtx> limiter = RequestLimiterBuilder.newBuilder()
                .named("limit_simulation")
                .metricRegistry(new LimitMetricRegistry()) //
                .blockOnLimit(true, 1000) //
                .build();

        Channel channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .usePlaintext() //
                .intercept(new ClientRequestLimitInterceptor(limiter)) //
                .build();

        AtomicLong counter = new AtomicLong();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                () -> System.out.println(" " + counter.getAndSet(0) + " : " + limiter.toString()), 1, 1, TimeUnit.SECONDS
        );

        for (int i = 0; i < 10000000; i++) {
            counter.incrementAndGet();
            ClientCalls.futureUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT), "request");
            if (i % 10000 == 0) {
                MetricsUtil.reportImmediately();
            }
        }
    }
}
