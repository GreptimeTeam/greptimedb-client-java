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

import com.google.protobuf.Message;
import io.greptime.common.SPI;

/**
 * GreptimeDB grpc impl service factory.
 *
 * @author jiachun.fjc
 */
@SPI
public class GrpcFactory implements RpcFactory {

    @SuppressWarnings("unchecked")
    @Override
    public void register(MethodDescriptor method, //
            Class<?> reqCls, //
            Object defaultReqIns, //
            Object defaultRespIns) {
        getMarshallerRegistry() //
                .registerMarshaller(method, (Class<? extends Message>) reqCls, (Message) defaultReqIns,
                        (Message) defaultRespIns);
    }

    @Override
    public RpcClient createRpcClient(ConfigHelper<RpcClient> helper) {
        RpcClient rpcClient = new GrpcClient(getMarshallerRegistry());
        if (helper != null) {
            helper.config(rpcClient);
        }
        return rpcClient;
    }

    protected MarshallerRegistry getMarshallerRegistry() {
        return MarshallerRegistry.DefaultMarshallerRegistry.INSTANCE;
    }
}
