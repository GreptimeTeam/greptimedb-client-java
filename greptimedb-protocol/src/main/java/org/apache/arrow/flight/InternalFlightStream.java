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
package org.apache.arrow.flight;

import java.util.concurrent.CompletableFuture;

/**
 * @author jiachun.fjc
 */
public class InternalFlightStream {

    private final FlightStream stream;
    private final CompletableFuture<Void> completed;

    public InternalFlightStream(FlightStream stream, CompletableFuture<Void> completed) {
        this.stream = stream;
        this.completed = completed;
    }

    public FlightStream getStream() {
        return stream;
    }

    public CompletableFuture<Void> getCompleted() {
        return completed;
    }
}
