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
package io.greptime.flight;

import com.google.protobuf.InvalidProtocolBufferException;
import io.greptime.common.Endpoint;
import io.greptime.rpc.Observer;
import io.greptime.v1.Database;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class GreptimeFlightClient implements AutoCloseable {

    private static final Logger          LOG              = LoggerFactory.getLogger(GreptimeFlightClient.class);

    private static final BufferAllocator BUFFER_ALLOCATOR = new RootAllocator(Integer.MAX_VALUE);

    private final Endpoint               endpoint;
    private final FlightClient           client;
    private final BufferAllocator        allocator;

    private GreptimeFlightClient(Endpoint endpoint, FlightClient client, BufferAllocator allocator) {
        this.endpoint = Objects.requireNonNull(endpoint);
        this.client = Objects.requireNonNull(client);
        this.allocator = Objects.requireNonNull(allocator);
    }

    public static GreptimeFlightClient createClient(Endpoint endpoint) {
        Location location = Location.forGrpcInsecure(endpoint.getAddr(), endpoint.getPort());

        String allocatorName = String.format("BufferAllocator(%s)", location);
        BufferAllocator allocator = BUFFER_ALLOCATOR.newChildAllocator(allocatorName, 0, Integer.MAX_VALUE);

        FlightClient.Builder builder = FlightClient.builder().location(location).allocator(allocator);
        FlightClient client = builder.build();

        return new GreptimeFlightClient(endpoint, client, allocator);
    }

    public FlightStream doRequest(GreptimeRequest request, CallOption... options) {
        Ticket ticket = request.into();
        return client.getStream(ticket, options);
    }

    // TODO(LFC): Handle errors in FlightStream, extract maybe existed errorCode and errorMsg from GRPC header.
    // The errorCode and errorMsg could be set by GreptimeDB server, which should be pass to observer's "onError".
    public void consumeStream(FlightStream flightStream, Observer<FlightMessage> messageObserver) {
        LOG.debug("Start consuming FlightStream ...");

        if (!flightStream.next()) {
            return;
        }

        if (extractAffectedRows(flightStream, messageObserver)) {
            return;
        }

        extractRecordbatch(flightStream, messageObserver);
    }

    private void extractRecordbatch(FlightStream flightStream, Observer<FlightMessage> messageObserver) {
        try (VectorSchemaRoot recordbatch = flightStream.getRoot()) {
            do {
                FlightMessage message = new FlightMessage(recordbatch);
                messageObserver.onNext(message);
            } while (flightStream.next());
        }
    }

    private boolean extractAffectedRows(FlightStream flightStream, Observer<FlightMessage> messageObserver) {
        ArrowBuf metadata = flightStream.getLatestMetadata();
        if (metadata != null) {
            byte[] buffer = new byte[(int) metadata.readableBytes()];
            metadata.readBytes(buffer);

            Database.FlightMetadata flightMetadata;
            try {
                flightMetadata = Database.FlightMetadata.parseFrom(buffer);
            } catch (InvalidProtocolBufferException e) {
                messageObserver.onError(e);
                return true;
            }

            if (flightMetadata.hasAffectedRows()) {
                int affectedRows = flightMetadata.getAffectedRows().getValue();
                FlightMessage message = new FlightMessage(affectedRows);
                messageObserver.onNext(message);
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "GreptimeFlightClient{endpoint=" + endpoint + ", client=" + client + ", allocator=" + allocator + '}';
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(client, allocator);
    }
}
