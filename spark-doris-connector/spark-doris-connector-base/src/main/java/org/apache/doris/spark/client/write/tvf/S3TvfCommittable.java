// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.client.write.tvf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Doris target, files, and columns prepared by one Spark task for a Doris INSERT. */
public final class S3TvfCommittable {
    private final String database;
    private final String table;
    private final String label;
    private final List<String> objectKeys;
    private final List<String> columns;

    public S3TvfCommittable(
            String database,
            String table,
            String label,
            List<String> objectKeys,
            List<String> columns) {
        this.database = database;
        this.table = table;
        this.label = label;
        this.objectKeys = Collections.unmodifiableList(new ArrayList<>(objectKeys));
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getLabel() {
        return label;
    }

    public List<String> getObjectKeys() {
        return objectKeys;
    }

    public List<String> getColumns() {
        return columns;
    }

    public boolean isEmpty() {
        return objectKeys.isEmpty();
    }
}
