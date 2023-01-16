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

import com.codahale.metrics.Histogram;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import io.greptime.Util;
import io.greptime.common.Endpoint;
import io.greptime.common.util.Clock;
import io.greptime.rpc.Context;
import io.greptime.v1.Database;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.ArrowBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Contains the success value of write.
 *
 * @author jiachun.fjc
 */
public class WriteOk {

    private static final Logger LOG = LoggerFactory.getLogger(WriteOk.class);

    private final Context ctx;
    private final Histogram writeRowsSuccess;
    private final FlightStream flightStream;
    private final SettableFuture<Integer> affectedRows = SettableFuture.create();

    public WriteOk(Context ctx, Histogram writeRowsSuccess, FlightStream flightStream) {
        this.ctx = ctx;
        this.writeRowsSuccess = writeRowsSuccess;
        this.flightStream = flightStream;
    }

    public int getSuccess() {
        try {
            return affectedRows.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isCompleted() {
        if (isFlightStreamCompleted()) {
            return true;
        }

        if (isFlightStreamQueueEmpty()) {
            return false;
        }

        if (!this.flightStream.next()) {
            return true;
        }

        Long writeId = ctx.get(Context.KEY_WRITE_ID);

        Long writeStart = ctx.get(Context.KEY_WRITE_START);
        if (Util.isRwLogging() && writeStart != null) {
            Endpoint endpoint = this.ctx.get(Context.KEY_ENDPOINT);
            LOG.info("[Write-{}] First time consuming data from {}, costs {} ms", writeId, endpoint, Clock
                    .defaultClock().duration(writeStart));
            ctx.remove(Context.KEY_WRITE_START);
        }

        ArrowBuf metadata = flightStream.getLatestMetadata();
        if (metadata == null) {
            this.affectedRows.setException(new IllegalStateException(
                    "Expecting server returns non-empty metadata as AffectedRows"));
            return true;
        }

        byte[] buffer = new byte[(int) metadata.readableBytes()];
        metadata.readBytes(buffer);

        Database.FlightMetadata flightMetadata;
        try {
            flightMetadata = Database.FlightMetadata.parseFrom(buffer);
        } catch (InvalidProtocolBufferException e) {
            this.affectedRows.setException(e);
            return true;
        }

        if (flightMetadata.hasAffectedRows()) {
            int value = flightMetadata.getAffectedRows().getValue();
            this.writeRowsSuccess.update(value);

            this.affectedRows.set(value);
            return true;
        }

        return false;
    }

    private boolean isFlightStreamQueueEmpty() {
        Field queueField;
        try {
            queueField = FlightStream.class.getDeclaredField("queue");
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
        queueField.setAccessible(true);

        LinkedBlockingQueue<?> queue;
        try {
            queue = (LinkedBlockingQueue<?>) queueField.get(this.flightStream);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        return queue.isEmpty();
    }

    private boolean isFlightStreamCompleted() {
        Field completedField;
        try {
            completedField = FlightStream.class.getDeclaredField("completed");
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
        completedField.setAccessible(true);

        CompletableFuture<?> completed;
        try {
            completed = (CompletableFuture<?>) completedField.get(this.flightStream);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        return completed.isDone();
    }
}
