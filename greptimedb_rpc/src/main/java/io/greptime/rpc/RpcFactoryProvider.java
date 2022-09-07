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

import io.greptime.common.util.ServiceLoader;

/**
 *
 * @author jiachun.fjc
 */
public class RpcFactoryProvider {

    /**
     * SPI RPC factory, default is GrpcFactory
     */
    private static final RpcFactory RPC_FACTORY = ServiceLoader.load(RpcFactory.class).first();

    /**
     * Get the {@link RpcFactory} impl, base on SPI.
     *
     * @return a shared rpcFactory instance
     */
    public static RpcFactory getRpcFactory() {
        return RPC_FACTORY;
    }
}
