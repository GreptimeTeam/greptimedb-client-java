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
package io.greptime.rpc.interceptors;

import com.netflix.concurrency.limits.Limiter;
import io.greptime.common.util.MetricsUtil;
import io.greptime.rpc.limit.LimitMetricRegistry;
import io.greptime.rpc.limit.RequestLimitCtx;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * ClientInterceptor that enforces per service and/or per method concurrent
 * request limits and returns a Status.UNAVAILABLE when that limit has been
 * reached.
 * <p>
 * Refer to `concurrency-limit-grpc`
 *
 * @author jiachun.fjc
 */
public class ClientRequestLimitInterceptor implements ClientInterceptor {

    private static final Status LIMIT_EXCEEDED_STATUS = Status.UNAVAILABLE.withDescription("Client limit reached");

    private static final AtomicBoolean LIMIT_SWITCH = new AtomicBoolean(true);

    private final Limiter<RequestLimitCtx> limiter;
    private final Function<String, Boolean> filter;

    public ClientRequestLimitInterceptor(Limiter<RequestLimitCtx> limiter) {
        this(limiter, (name) -> true);
    }

    public ClientRequestLimitInterceptor(Limiter<RequestLimitCtx> limiter, Function<String, Boolean> filter) {
        this.limiter = limiter;
        this.filter = filter;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, //
                                                               CallOptions callOpts, //
                                                               Channel next) {
        if (shouldNotUseLimiter(method.getType()) || !this.filter.apply(method.getFullMethodName())) {
            return next.newCall(method, callOpts);
        }

        String methodName = method.getFullMethodName();

        return MetricsUtil.timer(LimitMetricRegistry.RPC_LIMITER, "acquire_time", methodName)
                .timeSupplier(() -> this.limiter.acquire(() -> methodName))
                .map(listener -> (ClientCall<ReqT, RespT>) new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOpts)) {

                    private final AtomicBoolean done = new AtomicBoolean(false);

                    @Override
                    public void start(Listener<RespT> respListener, Metadata headers) {
                        super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(respListener) {

                            @Override
                            public void onClose(Status status, Metadata trailers) {
                                try {
                                    super.onClose(status, trailers);
                                } finally {
                                    if (done.compareAndSet(false, true)) {
                                        if (status.isOk()) {
                                            listener.onSuccess();
                                        } else if (Status.Code.UNAVAILABLE == status.getCode()) {
                                            listener.onDropped();
                                        } else {
                                            listener.onIgnore();
                                        }
                                    }
                                }
                            }
                        }, headers);
                    }

                    @Override
                    public void cancel(String message, Throwable cause) {
                        try {
                            super.cancel(message, cause);
                        } finally {
                            if (done.compareAndSet(false, true)) {
                                listener.onIgnore();
                            }
                        }
                    }
                })
                .orElseGet(() -> new ClientCall<ReqT, RespT>() {

                    private Listener<RespT> respListener;

                    @Override
                    public void start(Listener<RespT> respListener, Metadata headers) {
                        this.respListener = respListener;
                    }

                    @Override
                    public void request(int numMessages) {
                    }

                    @Override
                    public void cancel(String message, Throwable cause) {
                    }

                    @Override
                    public void halfClose() {
                        this.respListener.onClose(LIMIT_EXCEEDED_STATUS, new Metadata());
                    }

                    @Override
                    public void sendMessage(ReqT message) {
                    }
                });
    }

    /**
     * Whether limit open.
     *
     * @return true or false
     */
    public static boolean isLimitSwitchOpen() {
        return LIMIT_SWITCH.get();
    }

    /**
     * See {@link #isLimitSwitchOpen()}
     *
     * Reset `limitSwitch`, set to the opposite of the old value.
     *
     * @return old value
     */
    public static boolean resetLimitSwitch() {
        return LIMIT_SWITCH.getAndSet(!LIMIT_SWITCH.get());
    }

    private static boolean shouldNotUseLimiter(MethodDescriptor.MethodType methodType) {
        if (!isLimitSwitchOpen()) {
            return true;
        }

        return !methodType.clientSendsOneMessage() || !methodType.serverSendsOneMessage();
    }
}
