// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.client.write;

import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.util.RowConvertors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CopyIntoProcessor extends AbstractCopyIntoProcessor<InternalRow> {

    private StructType schema;

    public CopyIntoProcessor(DorisConfig config) throws Exception {
        super(config);
        this.schema = new StructType(new StructField[0]);
    }

    public CopyIntoProcessor(DorisConfig config, StructType schema) throws Exception {
        super(config);
        this.schema = schema;
    }

    @Override
    protected String toFormat(InternalRow row, String format) {
        switch (format) {
            case "csv":
                return RowConvertors.convertToCsv(row, schema, columnSeparator);
            case "json":
                return RowConvertors.convertToJson(row, schema);
            default:
                return null;
        }
    }

    public void setSchema(StructType schema) {
        this.schema = schema;
    }

}
