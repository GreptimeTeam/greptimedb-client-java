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
package com.google.protobuf;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@code ByteString} helper.
 *
 * @author jiachun.fjc
 */
public class ByteStringHelper {

    /**
     * Wrap a byte array into a ByteString.
     */
    public static ByteString wrap(byte[] bs) {
        return ByteString.wrap(bs);
    }

    /**
     * Wrap a byte array into a ByteString.
     *
     * @param bs the byte array
     * @param offset read start offset in array
     * @param len read data length
     * @return the result byte string.
     */
    public static ByteString wrap(byte[] bs, int offset, int len) {
        return ByteString.wrap(bs, offset, len);
    }

    /**
     * Wrap a byte buffer into a ByteString.
     */
    public static ByteString wrap(ByteBuffer buf) {
        return ByteString.wrap(buf);
    }

    /**
     * Steal the byte[] from {@link ByteString}, if failed,
     * then call {@link ByteString#toByteArray()}.
     *
     * @param byteString the byteString source data
     * @return carried bytes
     */
    public static byte[] sealByteArray(ByteString byteString) {
        BytesStealer stealer = new BytesStealer();
        try {
            byteString.writeTo(stealer);
            if (stealer.isValid()) {
                return stealer.value();
            }
        } catch (IOException ignored) {
            // ignored
        }
        return byteString.toByteArray();
    }
}
