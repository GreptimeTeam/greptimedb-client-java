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
package io.greptime.models;

import io.greptime.common.Endpoint;
import io.greptime.common.util.Strings;
import java.util.Collection;

/**
 * Contains the write/query error value.
 *
 * @author jiachun.fjc
 */
public class Err {
    // error code from server
    private int code;
    // error message
    private Throwable error;
    // the server address where the error occurred
    private Endpoint errTo;
    // the data of wrote failed, can be used to retry
    private Collection<WriteRows> rowsFailed;
    // the QL failed to query
    private String failedQl;

    /**
     * Returns the error code.
     */
    public int getCode() {
        return code;
    }

    /**
     * Returns the error.
     */
    public Throwable getError() {
        return error;
    }

    /**
     * Returns the server address where the error occurred.
     */
    public Endpoint getErrTo() {
        return errTo;
    }

    /**
     * Returns the data of wrote failed, can be used to retry.
     */
    public Collection<WriteRows> getRowsFailed() {
        return rowsFailed;
    }

    /**
     * Returns the QL failed to query.
     */
    public String getFailedQl() {
        return failedQl;
    }

    /**
     * Returns a {@link Result} containing this error.
     */
    public <T> Result<T, Err> mapToResult() {
        return Result.err(this);
    }

    @Override
    public String toString() {
        return "Err{" + //
                "code=" + code + //
                ", error='" + error + '\'' + //
                ", errTo=" + errTo + //
                ", failedQl=" + failedQl + //
                '}';
    }

    /**
     * Creates a new {@link Err} for write error.
     *
     * @param code the error code
     * @param error the error
     * @param errTo the server address where the error occurred
     * @param rowsFailed the data of wrote failed, can be used to retry
     * @return a new {@link Err} for write error
     */
    public static Err writeErr(int code, Throwable error, Endpoint errTo, Collection<WriteRows> rowsFailed) {
        Err err = new Err();
        err.code = code;
        err.error = error;
        err.errTo = errTo;
        err.rowsFailed = rowsFailed;
        return err;
    }

    /**
     * Creates a new {@link Err} for query error.
     *
     * @param code the error code
     * @param error the error
     * @param errTo the server address where the error occurred
     * @param failedQl the QL failed to query
     * @return a new {@link Err} for query error
     */
    public static Err queryErr(int code, Throwable error, Endpoint errTo, String failedQl) {
        Err err = new Err();
        err.code = code;
        err.error = error;
        err.errTo = errTo;
        err.failedQl = failedQl;
        return err;
    }
}
