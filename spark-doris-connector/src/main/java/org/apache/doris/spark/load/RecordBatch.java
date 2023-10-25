// Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.doris.spark.load;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Wrapper Object for batch loading
 */
public class RecordBatch {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    /**
     * Spark row data iterator
     */
    private final Iterator<InternalRow> iterator;

    /**
     * stream load format
     */
    private final String format;

    /**
     * column separator, only used when the format is csv
     */
    private final String sep;

    /**
     * line delimiter
     */
    private final byte[] delim;

    /**
     * schema of row
     */
    private final StructType schema;

    private final boolean addDoubleQuotes;

    private RecordBatch(Iterator<InternalRow> iterator, String format, String sep, byte[] delim,
                        StructType schema, boolean addDoubleQuotes) {
        this.iterator = iterator;
        this.format = format;
        this.sep = sep;
        this.delim = delim;
        this.schema = schema;
        this.addDoubleQuotes = addDoubleQuotes;
    }

    public Iterator<InternalRow> getIterator() {
        return iterator;
    }

    public String getFormat() {
        return format;
    }

    public String getSep() {
        return sep;
    }

    public byte[] getDelim() {
        return delim;
    }

    public StructType getSchema() {
        return schema;
    }

    public boolean getAddDoubleQuotes(){
        return addDoubleQuotes;
    }
    public static Builder newBuilder(Iterator<InternalRow> iterator) {
        return new Builder(iterator);
    }

    /**
     * RecordBatch Builder
     */
    public static class Builder {

        private final Iterator<InternalRow> iterator;

        private String format;

        private String sep;

        private byte[] delim;

        private StructType schema;

        private boolean addDoubleQuotes;

        public Builder(Iterator<InternalRow> iterator) {
            this.iterator = iterator;
        }

        public Builder format(String format) {
            this.format = format;
            return this;
        }

        public Builder sep(String sep) {
            this.sep = sep;
            return this;
        }

        public Builder delim(String delim) {
            this.delim = delim.getBytes(DEFAULT_CHARSET);
            return this;
        }

        public Builder schema(StructType schema) {
            this.schema = schema;
            return this;
        }

        public Builder addDoubleQuotes(boolean addDoubleQuotes) {
            this.addDoubleQuotes = addDoubleQuotes;
            return this;
        }

        public RecordBatch build() {
            return new RecordBatch(iterator, format, sep, delim, schema, addDoubleQuotes);
        }

    }

}
