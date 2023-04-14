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

import java.nio.ByteBuffer;

/**
 * Try to get the bytes form ByteString with no copy.
 * 
 * @author jiachun.fjc
 */
public class BytesStealer extends ByteOutput {

    private byte[] value;
    private boolean valid = false;

    public byte[] value() {
        return value;
    }

    public boolean isValid() {
        return valid;
    }

    @Override
    public void write(byte value) {
        this.valid = false;
    }

    @Override
    public void write(byte[] value, int offset, int length) {
        doWrite(value, offset, length);
    }

    @Override
    public void writeLazy(byte[] value, int offset, int length) {
        doWrite(value, offset, length);
    }

    @Override
    public void write(ByteBuffer value) {
        this.valid = false;
    }

    @Override
    public void writeLazy(ByteBuffer value) {
        this.valid = false;
    }

    private void doWrite(byte[] value, int offset, int length) {
        if (this.value != null) {
            this.valid = false;
            return;
        }
        if (offset == 0 && length == value.length) {
            this.value = value;
            this.valid = true;
        }
    }
}
