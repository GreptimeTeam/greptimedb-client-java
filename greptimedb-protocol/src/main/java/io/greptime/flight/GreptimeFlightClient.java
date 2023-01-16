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

import io.greptime.common.Endpoint;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;

public class GreptimeFlightClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GreptimeFlightClient.class);

    private static final BufferAllocator BUFFER_ALLOCATOR = new RootAllocator(Integer.MAX_VALUE);

    private final Endpoint endpoint;
    private final FlightClient client;
    private final BufferAllocator allocator;

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

        GreptimeFlightClient flightClient = new GreptimeFlightClient(endpoint, client, allocator);
        LOG.info("Created new {}", flightClient);
        return flightClient;
    }

    public FlightStream doRequest(GreptimeRequest request, CallOption... options) {
        Ticket ticket = request.into();
        return client.getStream(ticket, options);
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
