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

import com.google.protobuf.InvalidProtocolBufferException;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.TableName;
import io.greptime.models.WriteOk;
import io.greptime.v1.Database;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.InternalFlightStream;
import org.apache.arrow.memory.ArrowBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;

/**
 * @author jiachun.fjc
 */
public class CompletableWriteFuture extends CompletableFuture<Result<WriteOk, Err>> {

    private static final Logger LOG = LoggerFactory.getLogger(CompletableWriteFuture.class);

    private final TableName tableName;

    public CompletableWriteFuture(TableName tableName, InternalFlightStream flightStream) {
        this.tableName = tableName;
        listenOnCompleted(flightStream);
    }

    private void listenOnCompleted(InternalFlightStream flightStream) {
        CompletableFuture<Void> completed = flightStream.getCompleted();
        completed.whenComplete((r, e) -> {
            try (FlightStream stream = flightStream.getStream()) {
                if (e != null) {
                    completeExceptionally(e);
                } else {
                    stream.next();

                    ArrowBuf metadata = stream.getLatestMetadata();
                    if (metadata == null) {
                        completeExceptionally(new IllegalStateException("Expecting server returns non-empty metadata as AffectedRows"));
                        return;
                    }

                    byte[] buffer = new byte[(int) metadata.readableBytes()];
                    metadata.readBytes(buffer);

                    Database.FlightMetadata flightMetadata;
                    try {
                        flightMetadata = Database.FlightMetadata.parseFrom(buffer);
                    } catch (InvalidProtocolBufferException ex) {
                        completeExceptionally(ex);
                        return;
                    }

                    if (!flightMetadata.hasAffectedRows()) {
                        completeExceptionally(new IllegalStateException("Expecting server carry AffectedRows in metadata"));
                        return;
                    }

                    int value = flightMetadata.getAffectedRows().getValue();
                    complete(WriteOk.ok(value, 0, this.tableName).mapToResult());
                }
            } catch (Exception ex) {
                LOG.error("Failed to deal with `FlightStream`", ex);
            }
        });
    }
}
