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
@SuppressWarnings("unchecked")
public class Context implements Copiable<Context> {

    public static final String KEY_QUERY_ID = "QueryId";
    public static final String KEY_QUERY_START = "QueryStart";
    public static final String KEY_ENDPOINT = "Endpoint";

    private final Map<String, Object> ctx = new HashMap<>();

    /**
     * Creates a new {@link Context} with empty values.
     */
    public static Context newDefault() {
        return new Context();
    }

    /**
     * Creates a new {@link Context} with the specified key-value pair.
     *
     * @param key the key
     * @param value the value
     * @return the new {@link Context}
     */
    public static Context of(String key, Object value) {
        return new Context().with(key, value);
    }

    /**
     * Adds the specified key-value pair to this {@link Context}.
     *
     * @param key the key
     * @param value the value
     * @return this {@link Context}
     */
    public Context with(String key, Object value) {
        synchronized (this) {
            this.ctx.put(key, value);
        }
        return this;
    }

    /**
     * Gets the value of the specified key.
     *
     * @param key the key
     * @return the value
     * @param <T> the type of the value
     */
    public <T> T get(String key) {
        synchronized (this) {
            return (T) this.ctx.get(key);
        }
    }

    /**
     * Removes the specified key and its value form this {@link Context}.
     *
     * @param key the key
     * @return the removed value
     * @param <T> the type of the value
     */
    public <T> T remove(String key) {
        synchronized (this) {
            return (T) this.ctx.remove(key);
        }
    }

    /**
     * Gets the value of the specified key, or the default value if the key is not present.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value
     * @param <T> the type of the value
     */
    @SuppressWarnings("unused")
    public <T> T getOrDefault(String key, T defaultValue) {
        synchronized (this) {
            return (T) this.ctx.getOrDefault(key, defaultValue);
        }
    }

    /**
     * Clears all key-value pairs from this {@link Context}.
     */
    @SuppressWarnings("unused")
    public void clear() {
        synchronized (this) {
            this.ctx.clear();
        }
    }

    /**
     * Returns all the KV entries in this {@link Context}.
     */
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
