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
package org.apache.arrow.flight;

import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.arrow.flight.auth.ClientAuthInterceptor;
import org.apache.arrow.flight.grpc.ClientInterceptorAdapter;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceBlockingStub;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceStub;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import javax.net.ssl.SSLException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client for Flight services.
 *
 * Refer to https://github.com/apache/arrow/blob/master/java/flight/flight-core/src/main/java/org/apache/arrow/flight/FlightClient.java
 */
@SuppressWarnings("all")
public class InternalFlightClient implements AutoCloseable {
    private static final int PENDING_REQUESTS = 5;
    /** The maximum number of trace events to keep on the gRPC Channel. This value disables channel tracing. */
    private static final int MAX_CHANNEL_TRACE_EVENTS = 0;
    private final BufferAllocator allocator;
    private final ManagedChannel channel;
    private final Channel interceptedChannel;
    private final FlightServiceBlockingStub blockingStub;
    private final FlightServiceStub asyncStub;
    private final ClientAuthInterceptor authInterceptor = new ClientAuthInterceptor();
    private final MethodDescriptor<Flight.Ticket, ArrowMessage> doGetDescriptor;
    private final MethodDescriptor<ArrowMessage, Flight.PutResult> doPutDescriptor;
    private final MethodDescriptor<ArrowMessage, ArrowMessage> doExchangeDescriptor;
    private final List<FlightClientMiddleware.Factory> middleware;

    /**
     * Create a Flight client from an allocator and a gRPC channel.
     */
    public InternalFlightClient(BufferAllocator incomingAllocator, ManagedChannel channel,
            List<FlightClientMiddleware.Factory> middleware) {
        this.allocator = incomingAllocator.newChildAllocator("flight-client", 0, Long.MAX_VALUE);
        this.channel = channel;
        this.middleware = middleware;

        final ClientInterceptor[] interceptors;
        interceptors = new ClientInterceptor[] {authInterceptor, new ClientInterceptorAdapter(middleware)};

        // Create a channel with interceptors pre-applied for DoGet and DoPut
        this.interceptedChannel = ClientInterceptors.intercept(channel, interceptors);

        blockingStub = FlightServiceGrpc.newBlockingStub(interceptedChannel);
        asyncStub = FlightServiceGrpc.newStub(interceptedChannel);
        doGetDescriptor = FlightBindingService.getDoGetDescriptor(allocator);
        doPutDescriptor = FlightBindingService.getDoPutDescriptor(allocator);
        doExchangeDescriptor = FlightBindingService.getDoExchangeDescriptor(allocator);
    }


    /**
     * Retrieve a stream from the server.
     * @param ticket The ticket granting access to the data stream.
     * @param options RPC-layer hints for this call.
     */
    public InternalFlightStream getStream(Ticket ticket, CallOption... options) {
    final io.grpc.CallOptions callOptions = CallOptions.wrapStub(asyncStub, options).getCallOptions();
    ClientCall<Flight.Ticket, ArrowMessage> call = interceptedChannel.newCall(doGetDescriptor, callOptions);
    FlightStream stream = new FlightStream(
        allocator,
        PENDING_REQUESTS,
            call::cancel,
            call::request);

    final StreamObserver<ArrowMessage> delegate = stream.asObserver();
    final CompletableFuture<Void> completed = new CompletableFuture<>();
    ClientResponseObserver<Flight.Ticket, ArrowMessage> clientResponseObserver =
        new ClientResponseObserver<Flight.Ticket, ArrowMessage>() {

          @Override
          public void beforeStart(ClientCallStreamObserver<org.apache.arrow.flight.impl.Flight.Ticket> requestStream) {
            requestStream.disableAutoInboundFlowControl();
          }

          @Override
          public void onNext(ArrowMessage value) {
            delegate.onNext(value);
          }

          @Override
          public void onError(Throwable t) {
            delegate.onError(StatusUtils.toGrpcException(t));
            completed.completeExceptionally(t);
          }

          @Override
          public void onCompleted() {
            delegate.onCompleted();
            completed.complete(null);
          }
        };

    ClientCalls.asyncServerStreamingCall(call, ticket.toProtocol(), clientResponseObserver);
    return new InternalFlightStream(stream, completed);
  }

    @Override
    public void close() throws Exception {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        allocator.close();
    }

    /**
     * Create a builder for a Flight client.
     */
    public static InternalFlightClient.Builder builder() {
        return new InternalFlightClient.Builder();
    }

    /**
     * A builder for Flight clients.
     */
    public static final class Builder {
        private BufferAllocator allocator;
        private Location location;
        private boolean forceTls = false;
        private int maxInboundMessageSize = FlightServer.MAX_GRPC_MESSAGE_SIZE;
        private InputStream trustedCertificates = null;
        private InputStream clientCertificate = null;
        private InputStream clientKey = null;
        private String overrideHostname = null;
        private List<FlightClientMiddleware.Factory> middleware = new ArrayList<>();
        private boolean verifyServer = true;

        private Builder() {}

        private Builder(BufferAllocator allocator, Location location) {
            this.allocator = Preconditions.checkNotNull(allocator);
            this.location = Preconditions.checkNotNull(location);
        }

        /**
         * Force the client to connect over TLS.
         */
        public Builder useTls() {
            this.forceTls = true;
            return this;
        }

        /** Override the hostname checked for TLS. Use with caution in production. */
        public Builder overrideHostname(final String hostname) {
            this.overrideHostname = hostname;
            return this;
        }

        /** Set the maximum inbound message size. */
        public Builder maxInboundMessageSize(int maxSize) {
            Preconditions.checkArgument(maxSize > 0);
            this.maxInboundMessageSize = maxSize;
            return this;
        }

        /** Set the trusted TLS certificates. */
        public Builder trustedCertificates(final InputStream stream) {
            this.trustedCertificates = Preconditions.checkNotNull(stream);
            return this;
        }

        /** Set the trusted TLS certificates. */
        public Builder clientCertificate(final InputStream clientCertificate, final InputStream clientKey) {
            Preconditions.checkNotNull(clientKey);
            this.clientCertificate = Preconditions.checkNotNull(clientCertificate);
            this.clientKey = Preconditions.checkNotNull(clientKey);
            return this;
        }

        public Builder allocator(BufferAllocator allocator) {
            this.allocator = Preconditions.checkNotNull(allocator);
            return this;
        }

        public Builder location(Location location) {
            this.location = Preconditions.checkNotNull(location);
            return this;
        }

        public Builder intercept(FlightClientMiddleware.Factory factory) {
            middleware.add(factory);
            return this;
        }

        public Builder verifyServer(boolean verifyServer) {
            this.verifyServer = verifyServer;
            return this;
        }

        /**
         * Create the client from this builder.
         */
        public InternalFlightClient build() {
            final NettyChannelBuilder builder;

            switch (location.getUri().getScheme()) {
                case LocationSchemes.GRPC:
                case LocationSchemes.GRPC_INSECURE:
                case LocationSchemes.GRPC_TLS: {
                    builder = NettyChannelBuilder.forAddress(location.toSocketAddress());
                    break;
                }
                case LocationSchemes.GRPC_DOMAIN_SOCKET: {
                    // The implementation is platform-specific, so we have to find the classes at runtime
                    builder = NettyChannelBuilder.forAddress(location.toSocketAddress());
                    try {
                        try {
                            // Linux
                            builder.channelType((Class<? extends ServerChannel>) Class
                                    .forName("io.netty.channel.epoll.EpollDomainSocketChannel"));
                            final EventLoopGroup elg =
                                    (EventLoopGroup) Class.forName("io.netty.channel.epoll.EpollEventLoopGroup")
                                            .newInstance();
                            builder.eventLoopGroup(elg);
                        } catch (ClassNotFoundException e) {
                            // BSD
                            builder.channelType((Class<? extends ServerChannel>) Class
                                    .forName("io.netty.channel.kqueue.KQueueDomainSocketChannel"));
                            final EventLoopGroup elg =
                                    (EventLoopGroup) Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup")
                                            .newInstance();
                            builder.eventLoopGroup(elg);
                        }
                    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                        throw new UnsupportedOperationException(
                                "Could not find suitable Netty native transport implementation for domain socket address.");
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("Scheme is not supported: " + location.getUri().getScheme());
            }

            if (this.forceTls || LocationSchemes.GRPC_TLS.equals(location.getUri().getScheme())) {
                builder.useTransportSecurity();

                final boolean hasTrustedCerts = this.trustedCertificates != null;
                final boolean hasKeyCertPair = this.clientCertificate != null && this.clientKey != null;
                if (!this.verifyServer && (hasTrustedCerts || hasKeyCertPair)) {
                    throw new IllegalArgumentException(
                            "FlightClient has been configured to disable server verification, "
                                    + "but certificate options have been specified.");
                }

                final SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();

                if (!this.verifyServer) {
                    sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else if (this.trustedCertificates != null || this.clientCertificate != null || this.clientKey != null) {
                    if (this.trustedCertificates != null) {
                        sslContextBuilder.trustManager(this.trustedCertificates);
                    }
                    if (this.clientCertificate != null && this.clientKey != null) {
                        sslContextBuilder.keyManager(this.clientCertificate, this.clientKey);
                    }
                }
                try {
                    builder.sslContext(sslContextBuilder.build());
                } catch (SSLException e) {
                    throw new RuntimeException(e);
                }

                if (this.overrideHostname != null) {
                    builder.overrideAuthority(this.overrideHostname);
                }
            } else {
                builder.usePlaintext();
            }

            builder.maxTraceEvents(MAX_CHANNEL_TRACE_EVENTS).maxInboundMessageSize(maxInboundMessageSize);
            return new InternalFlightClient(allocator, builder.build(), middleware);
        }
    }
}
