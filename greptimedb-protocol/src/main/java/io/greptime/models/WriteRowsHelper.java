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

import io.greptime.common.util.Ensures;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import java.util.Collection;
import java.util.Collections;

/**
 * @author jiachun.fjc
 */
public class WriteRowsHelper {

    public static Database.GreptimeRequest toGreptimeRequest(WriteRows rows, AuthInfo authInfo) {
        return toGreptimeRequest(Collections.singleton(rows.tableName()), Collections.singleton(rows), authInfo);
    }

    public static Database.GreptimeRequest toGreptimeRequest(Collection<TableName> tableNames,
            Collection<WriteRows> rows, AuthInfo authInfo) {
        String dbName = null;
        for (TableName t : tableNames) {
            if (dbName == null) {
                dbName = t.getDatabaseName();
            } else if (!dbName.equals(t.getDatabaseName())) {
                String errMsg =
                        String.format("Write to multiple databases is not supported: %s, %s", dbName,
                                t.getDatabaseName());
                throw new IllegalArgumentException(errMsg);
            }
        }

        Common.RequestHeader.Builder headerBuilder = Common.RequestHeader.newBuilder();
        if (dbName != null) {
            headerBuilder.setDbname(dbName);
        }
        if (authInfo != null) {
            headerBuilder.setAuthorization(authInfo.into());
        }

        Database.InsertRequests.Builder insertRequestsBuilder = Database.InsertRequests.newBuilder();
        Database.RowInsertRequests.Builder rowInsertRequestsBuilder = Database.RowInsertRequests.newBuilder();
        for (WriteRows r : rows) {
            switch (r.writeProtocol()) {
                case Columnar:
                    insertRequestsBuilder.addInserts(r.intoColumnarInsertRequest());
                    break;
                case Row:
                    rowInsertRequestsBuilder.addInserts(r.intoRowInsertRequest());
                    break;
            }
        }

        if (insertRequestsBuilder.getInsertsCount() > 0) {
            Ensures.ensure(rowInsertRequestsBuilder.getInsertsCount() == 0, "Columnar and Row inserts cannot be mixed");
            return Database.GreptimeRequest.newBuilder() //
                    .setHeader(headerBuilder.build()) //
                    .setInserts(insertRequestsBuilder.build()) //
                    .build();
        } else if (rowInsertRequestsBuilder.getInsertsCount() > 0) {
            Ensures.ensure(insertRequestsBuilder.getInsertsCount() == 0, "Columnar and Row inserts cannot be mixed");
            return Database.GreptimeRequest.newBuilder() //
                    .setHeader(headerBuilder.build()) //
                    .setRowInserts(rowInsertRequestsBuilder.build()) //
                    .build();
        }
        throw new IllegalArgumentException("No inserts");
    }

    private WriteRowsHelper() {}
}
