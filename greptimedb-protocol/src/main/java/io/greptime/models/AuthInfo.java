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

import io.greptime.common.Into;
import io.greptime.v1.Common;

/**
 * Greptime authentication information
 *
 * @author sunng87
 */
public class AuthInfo implements Into<Common.AuthHeader> {

    private String username;
    private String password;

    /**
     * Create AuthInfo from username/password.
     */
    public AuthInfo(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public Common.AuthHeader into() {
        Common.Basic basic = Common.Basic.newBuilder() //
                .setUsername(getUsername()) //
                .setPassword(getPassword()) //
                .build();
        return Common.AuthHeader.newBuilder() //
                .setBasic(basic) //
                .build();
    }
}
