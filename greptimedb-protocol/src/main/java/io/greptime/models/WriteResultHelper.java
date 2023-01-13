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

import io.greptime.Status;
import io.greptime.common.Endpoint;
import io.greptime.flight.FlightMessage;
import io.greptime.rpc.Observer;

/**
 * @author jiachun.fjc
 */
public final class WriteResultHelper implements Observer<FlightMessage> {

    private final Endpoint endpoint;
    private final WriteRows writeRows;
    private Result<WriteOk, Err> result;

    public WriteResultHelper(Endpoint endpoint, WriteRows writeRows) {
        this.endpoint = endpoint;
        this.writeRows = writeRows;
    }

    @Override
    public void onNext(FlightMessage message) {
        if (message.getType() != FlightMessage.Type.AffectedRows) {
            IllegalStateException error =
                    new IllegalStateException("Expect server returns affected rows message, actual: "
                            + message.getType().name());
            result = Err.writeErr(Status.Unexpected.getStatusCode(), error, endpoint, writeRows).mapToResult();
        } else {
            result = WriteOk.ok(message.getAffectedRows(), 0, writeRows.tableName()).mapToResult();
        }
    }

    @Override
    public void onError(Throwable err) {
        result = Err.writeErr(Status.Unknown.getStatusCode(), err, endpoint, writeRows).mapToResult();
    }

    public Result<WriteOk, Err> getResult() {
        return result;
    }
}
