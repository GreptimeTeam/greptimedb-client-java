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
package io.greptime.rpc;

import io.greptime.common.Display;
import io.greptime.common.Endpoint;
import io.greptime.common.Lifecycle;
import io.greptime.rpc.errors.RemotingException;

/**
 * A common RPC client interface.
 *
 * @author jiachun.fjc
 */
@SuppressWarnings("unused")
public interface RpcClient extends Lifecycle<RpcOptions>, Display {

    /**
     * Check connection for given address.
     *
     * @param endpoint target address
     * @return true if there is a connection adn the connection is active adn writable.
     */
    boolean checkConnection(Endpoint endpoint);

    /**
     * Check connection for given address and async to create a new one if there is no connection.
     *
     * @param endpoint target address
     * @param createIfAbsent create a new one if there is no connection
     * @return true if there is a connection and the connection is active and writable.
     */
    boolean checkConnection(Endpoint endpoint, boolean createIfAbsent);

    /**
     * Close all connections of an address.
     *
     * @param endpoint target address
     */
    void closeConnection(Endpoint endpoint);

    /**
     * Register a connection state observer.
     *
     * @param observer connection state observer
     */
    void registerConnectionObserver(ConnectionObserver observer);

    /**
     * A connection observer.
     */
    interface ConnectionObserver {

        /**
         * The channel has successfully established a connection.
         *
         * @param ep the server endpoint
         */
        void onReady(Endpoint ep);

        /**
         * There has been some transient failure (such as a TCP 3-way handshake timing
         * out or a socket error).
         *
         * @param ep the server endpoint
         */
        void onFailure(Endpoint ep);

        /**
         * This channel has started shutting down. Any new RPCs should fail immediately.
         *
         * @param ep the server endpoint
         */
        void onShutdown(Endpoint ep);
    }

    /**
     * Executes a synchronous call.
     *
     * @param endpoint target address
     * @param request request object
     * @param timeoutMs timeout millisecond
     * @param <Req> request message type
     * @param <Resp> response message type
     * @return response
     */
    default <Req, Resp> Resp invokeSync(Endpoint endpoint,
                                        Req request,
                                        long timeoutMs) throws RemotingException {
        return invokeSync(endpoint, request, null, timeoutMs);
    }

    /**
     * Executes a synchronous call using an invoke context.
     *
     * @param endpoint target address
     * @param request request object
     * @param ctx invoke context
     * @param timeoutMs timeout millisecond
     * @param <Req> request message type
     * @param <Resp> response message type
     * @return response
     */
    <Req, Resp> Resp invokeSync(Endpoint endpoint,
                                Req request,
                                Context ctx,
                                long timeoutMs) throws RemotingException;

    /**
     * Executes an asynchronous call with a response {@link Observer}.
     *
     * @param endpoint target address
     * @param request request object
     * @param observer response observer
     * @param timeoutMs timeout millisecond
     * @param <Req> request message type
     * @param <Resp> response message type
     */
    default <Req, Resp> void invokeAsync(Endpoint endpoint,
                                         Req request,
                                         Observer<Resp> observer,
                                         long timeoutMs) throws RemotingException {
        invokeAsync(endpoint, request, null, observer, timeoutMs);
    }

    /**
     * Executes an asynchronous call with a response {@link Observer}.
     *
     * @param endpoint target address
     * @param request request object
     * @param ctx invoke context
     * @param observer response observer
     * @param timeoutMs timeout millisecond
     * @param <Req> request message type
     * @param <Resp> response message type
     */
    <Req, Resp> void invokeAsync(Endpoint endpoint,
                                 Req request,
                                 Context ctx,
                                 Observer<Resp> observer,
                                 long timeoutMs) throws RemotingException;

    /**
     * Executes a server-streaming call with a response {@link Observer}.
     * <p>
     * One request message followed by zero or more response messages.
     *
     * @param endpoint target address
     * @param request request object
     * @param ctx invoke context
     * @param observer response stream observer
     * @param <Req> request message type
     * @param <Resp> response message type
     */
    <Req, Resp> void invokeServerStreaming(Endpoint endpoint,
                                           Req request,
                                           Context ctx,
                                           Observer<Resp> observer) throws RemotingException;

    /**
     * Executes a client-streaming call with a request {@link Observer}
     * and a response {@link Observer}.
     *
     * @param endpoint target address
     * @param defaultReqIns the default request instance
     * @param ctx invoke context
     * @param respObserver response stream observer
     * @param <Req> request message type
     * @param <Resp> response message type
     * @return request {@link Observer}.
     */
    <Req, Resp> Observer<Req> invokeClientStreaming(Endpoint endpoint,
                                                    Req defaultReqIns,
                                                    Context ctx,
                                                    Observer<Resp> respObserver) throws RemotingException;
}
