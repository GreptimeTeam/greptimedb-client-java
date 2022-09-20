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
package io.greptime;

import io.greptime.rpc.MethodDescriptor;
import io.greptime.rpc.RpcFactoryProvider;
import io.greptime.v1.GreptimeDB;

/**
 *
 * @author jiachun.fjc
 */
public class RpcServiceRegister {

    private static final String METHOD_TEMPLATE = "greptime.v1.Greptime/%s";

    public static void registerAllService() {
        // register protobuf serializer
        RpcFactoryProvider.getRpcFactory().register(
            MethodDescriptor.of(String.format(METHOD_TEMPLATE, "Batch"), MethodDescriptor.MethodType.UNARY, 1), //
            GreptimeDB.BatchRequest.class, //
            GreptimeDB.BatchRequest.getDefaultInstance(), //
            GreptimeDB.BatchResponse.getDefaultInstance());
    }
}
