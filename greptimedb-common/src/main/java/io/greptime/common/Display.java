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

import java.io.PrintWriter;

/**
 * Components that implement this interface need to be able to display
 * their own state and output state information via the {@code display}
 * method.
 *
 * @author jiachun.fjc
 */
public interface Display {

    /**
     * Display self state.
     *
     * @param out output
     */
    void display(Printer out);

    interface Printer {

        /**
         * Prints an object.
         *
         * @param x The <code>Object</code> to be printed
         * @return this printer
         */
        Printer print(Object x);

        /**
         * Prints an Object and then terminates the line.
         *
         * @param x The <code>Object</code> to be printed.
         * @return this printer
         */
        Printer println(Object x);
    }

    class DefaultPrinter implements Printer {

        private final PrintWriter out;

        public DefaultPrinter(PrintWriter out) {
            this.out = out;
        }

        @Override
        public Printer print(Object x) {
            this.out.print(x);
            return this;
        }

        @Override
        public Printer println(Object x) {
            this.out.println(x);
            return this;
        }
    }
}
