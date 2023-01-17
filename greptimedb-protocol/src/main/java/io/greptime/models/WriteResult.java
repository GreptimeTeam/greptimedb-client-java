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
package io.greptime.models;

import com.google.protobuf.InvalidProtocolBufferException;
import io.greptime.Status;
import io.greptime.Util;
import io.greptime.common.Endpoint;
import io.greptime.common.util.Clock;
import io.greptime.rpc.Context;
import io.greptime.v1.Database;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.ArrowBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteResult {

    private static final Logger LOG = LoggerFactory.getLogger(WriteResult.class);

    private final Context ctx;
    private final FlightStream flightStream;
    private final WriteRows writeRows;

    public WriteResult(Context ctx, FlightStream flightStream, WriteRows writeRows) {
        this.ctx = ctx;
        this.flightStream = flightStream;
        this.writeRows = writeRows;
    }

    public Result<WriteOk, Err> get() {
        LOG.debug("Start consuming FlightStream as write result ...");

        this.flightStream.next();

        Endpoint endpoint = this.ctx.get(Context.KEY_ENDPOINT);

        if (Util.isRwLogging()) {
            Long writeId = this.ctx.get(Context.KEY_WRITE_ID);

            Long writeStart = this.ctx.get(Context.KEY_WRITE_START);
            long duration = Clock.defaultClock().duration(writeStart);

            LOG.info("[Write-{}](to {}) Duration from write start: {} ms", writeId, endpoint, duration);
        }

        ArrowBuf metadata = this.flightStream.getLatestMetadata();
        if (metadata == null) {
            IllegalStateException error =
                    new IllegalStateException("Expecting server returns non-empty metadata as AffectedRows");
            return Err.writeErr(Status.Unknown.getStatusCode(), error, endpoint, this.writeRows).mapToResult();
        }

        byte[] buffer = new byte[(int) metadata.readableBytes()];
        metadata.readBytes(buffer);

        Database.FlightMetadata flightMetadata;
        try {
            flightMetadata = Database.FlightMetadata.parseFrom(buffer);
        } catch (InvalidProtocolBufferException e) {
            return Err.writeErr(Status.Unknown.getStatusCode(), e, endpoint, this.writeRows).mapToResult();
        }

        if (!flightMetadata.hasAffectedRows()) {
            IllegalStateException error = new IllegalStateException("Expecting server carry AffectedRows in metadata");
            return Err.writeErr(Status.Unknown.getStatusCode(), error, endpoint, this.writeRows).mapToResult();
        }

        int value = flightMetadata.getAffectedRows().getValue();
        return WriteOk.ok(value, 0, this.writeRows.tableName()).mapToResult();
    }
}
