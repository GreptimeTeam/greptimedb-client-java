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
package io.greptime.common.util;

import io.greptime.common.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 *
 * @author jiachun.fjc
 */
public class Platform {

    private static final Logger  LOG        = LoggerFactory.getLogger(Platform.class);

    private static final String  WIN_KEY    = "win";
    private static final String  MAC_KEY    = "mac os x";

    private static final boolean IS_WINDOWS = isWindows0();
    private static final boolean IS_MAC     = isMac0();

    /**
     * Return {@code true} if the JVM is running on Windows
     */
    public static boolean isWindows() {
        return IS_WINDOWS;
    }

    /**
     * Return {@code true} if the JVM is running on Mac OSX
     */
    public static boolean isMac() {
        return IS_MAC;
    }

    private static boolean isMac0() {
        boolean mac = checkOS(MAC_KEY);
        if (mac) {
            LOG.debug("Platform: Mac OS X");
        }
        return mac;
    }

    private static boolean isWindows0() {
        boolean windows = checkOS(WIN_KEY);
        if (windows) {
            LOG.debug("Platform: Windows");
        }
        return windows;
    }

    private static boolean checkOS(String osKey) {
        return SystemPropertyUtil.get(Keys.OS_NAME, "") //
            .toLowerCase(Locale.US) //
            .contains(osKey);
    }
}
