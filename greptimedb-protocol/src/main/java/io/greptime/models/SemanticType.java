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
package io.greptime.models;

import io.greptime.v1.Columns;

/**
 * @author jiachun.fjc
 */
public enum SemanticType {
    Tag, Field, Timestamp;

    public Columns.Column.SemanticType toProtoValue() {
        switch (this) {
            case Tag:
                return Columns.Column.SemanticType.TAG;
            case Field:
                return Columns.Column.SemanticType.FIELD;
            case Timestamp:
                return Columns.Column.SemanticType.TIMESTAMP;
            default:
                return null;
        }
    }

    public static SemanticType fromProtoValue(Columns.Column.SemanticType v) {
        switch (v) {
            case TAG:
                return Tag;
            case FIELD:
                return Field;
            case TIMESTAMP:
                return Timestamp;
            default:
                return null;
        }
    }
}
