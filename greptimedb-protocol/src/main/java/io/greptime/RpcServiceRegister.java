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
package io.greptime;

import io.greptime.rpc.MethodDescriptor;
import io.greptime.rpc.RpcFactoryProvider;
import io.greptime.v1.Database;

/**
 * The RPC service register.
 *
 * @author jiachun.fjc
 */
public class RpcServiceRegister {

    private static final String METHOD_TEMPLATE = "greptime.v1.GreptimeDatabase/%s";

    public static void registerAllService() {
        // register protobuf serializer
        RpcFactoryProvider.getRpcFactory().register(
                MethodDescriptor.of(String.format(METHOD_TEMPLATE, "Handle"), MethodDescriptor.MethodType.UNARY, 1), //
                Database.GreptimeRequest.class, //
                Database.GreptimeRequest.getDefaultInstance(), //
                Database.GreptimeResponse.getDefaultInstance());

        RpcFactoryProvider.getRpcFactory().register(
                MethodDescriptor.of(String.format(METHOD_TEMPLATE, "HandleRequests"),
                        MethodDescriptor.MethodType.CLIENT_STREAMING), //
                Database.GreptimeRequest.class, //
                Database.GreptimeRequest.getDefaultInstance(), //
                Database.GreptimeResponse.getDefaultInstance());
    }
}
