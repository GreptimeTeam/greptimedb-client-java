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
package io.greptime.rpc;

import io.greptime.common.Copiable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Invoke context.
 *
 * @author jiachun.fjc
 */
@SuppressWarnings({ "unchecked", "unused" })
public class Context implements Copiable<Context> {

    public static final String KEY_QUERY_ID = "QueryId";
    public static final String KEY_QUERY_START = "QueryStart";
    public static final String KEY_WRITE_ID = "WriteId";
    public static final String KEY_WRITE_START = "WriteStart";
    public static final String KEY_ENDPOINT = "Endpoint";

    private final Map<String, Object> ctx             = new HashMap<>();

    public static Context newDefault() {
        return new Context();
    }

    public static Context of(String key, Object value) {
        return new Context().with(key, value);
    }

    public Context with(String key, Object value) {
        synchronized (this) {
            this.ctx.put(key, value);
        }
        return this;
    }

    /**
     * Light-weight(no synchronization involved) check whether this context contains the provided key.
     */
    public boolean has(String key) {
        return this.ctx.containsKey(key);
    }

    public <T> T get(String key) {
        synchronized (this) {
            return (T) this.ctx.get(key);
        }
    }

    public <T> T remove(String key) {
        synchronized (this) {
            return (T) this.ctx.remove(key);
        }
    }

    public <T> T getOrDefault(String key, T defaultValue) {
        synchronized (this) {
            return (T) this.ctx.getOrDefault(key, defaultValue);
        }
    }

    public void clear() {
        synchronized (this) {
            this.ctx.clear();
        }
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        synchronized (this) {
            return this.ctx.entrySet();
        }
    }

    @Override
    public String toString() {
        synchronized (this) {
            return this.ctx.toString();
        }
    }

    @Override
    public Context copy() {
        synchronized (this) {
            Context copy = new Context();
            copy.ctx.putAll(this.ctx);
            return copy;
        }
    }
}
