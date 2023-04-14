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
package io.greptime.common.util;

/**
 * Reuse {@link StringBuilder} based on {@link ThreadLocal}.
 * <p>
 * Be careful that do not to nest in the same thread.
 *
 * @author jiachun.fjc
 */
@SuppressWarnings("unused")
public class StringBuilderHelper {

    private static final int                              MAX_BUF_SIZE;
    private static final ThreadLocal<StringBuilderHolder> HOLDER_THREAD_LOCAL;

    static {
        MAX_BUF_SIZE = 1024 << 3;     // 8k
        HOLDER_THREAD_LOCAL = ThreadLocal.withInitial(StringBuilderHolder::new);
    }

    public static StringBuilder get() {
        StringBuilderHolder holder = HOLDER_THREAD_LOCAL.get();
        return holder.getStringBuilder();
    }

    public static void truncate() {
        StringBuilderHolder holder = HOLDER_THREAD_LOCAL.get();
        holder.truncate();
    }

    private static class StringBuilderHolder {

        private StringBuilder buf = new StringBuilder();

        private StringBuilder getStringBuilder() {
            truncate();
            return this.buf;
        }

        private void truncate() {
            if (this.buf.capacity() < MAX_BUF_SIZE) {
                this.buf.setLength(0); // reuse
            } else {
                this.buf = new StringBuilder();
            }
        }
    }
}
