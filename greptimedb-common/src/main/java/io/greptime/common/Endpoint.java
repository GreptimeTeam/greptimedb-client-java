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
package io.greptime.common;

import io.greptime.common.util.Strings;
import java.io.Serializable;

/**
 * An IP address with port.
 *
 * @author jiachun.fjc
 */
public class Endpoint implements Serializable {

    private static final long serialVersionUID = -7329681263115546100L;

    private final String addr;
    private final int port;

    public static Endpoint of(String addr, int port) {
        return new Endpoint(addr, port);
    }

    public static Endpoint parse(String s) {
        if (Strings.isNullOrEmpty(s)) {
            return null;
        }

        String[] arr = Strings.split(s, ':');

        if (arr == null || arr.length < 2) {
            return null;
        }

        try {
            int port = Integer.parseInt(arr[1]);
            return Endpoint.of(arr[0], port);
        } catch (Exception ignored) {
            return null;
        }
    }

    public Endpoint(String addr, int port) {
        super();
        this.addr = addr;
        this.port = port;
    }

    public String getAddr() {
        return addr;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return this.addr + ":" + this.port;
    }

    @Override
    public int hashCode() {
        int prime = 31;
        int result = 1;
        result = prime * result + (this.addr == null ? 0 : this.addr.hashCode());
        result = prime * result + this.port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Endpoint other = (Endpoint) obj;
        if (this.addr == null) {
            if (other.addr != null) {
                return false;
            }
        } else if (!this.addr.equals(other.addr)) {
            return false;
        }
        return this.port == other.port;
    }
}
