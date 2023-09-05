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

import io.greptime.v1.Columns;
import io.greptime.v1.Common;

/**
 * The semantic type.
 *
 * @author jiachun.fjc
 */
public enum SemanticType {
    Tag, Field, Timestamp;

    /**
     * Converts to the corresponding proto value.
     */
    public Common.SemanticType toProtoValue() {
        switch (this) {
            case Tag:
                return Common.SemanticType.TAG;
            case Field:
                return Common.SemanticType.FIELD;
            case Timestamp:
                return Common.SemanticType.TIMESTAMP;
            default:
                return null;
        }
    }

    /**
     * Converts from the corresponding proto value.
     */
    public static SemanticType fromProtoValue(Common.SemanticType v) {
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
