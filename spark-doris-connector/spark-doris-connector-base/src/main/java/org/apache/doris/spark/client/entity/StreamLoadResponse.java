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

package org.apache.doris.spark.client.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamLoadResponse {

    private final static List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));

    @JsonProperty(value = "TxnId")
    private long TxnId;

    @JsonProperty(value = "msg")
    private String msg;

    @JsonProperty(value = "Label")
    private String Label;

    @JsonProperty(value = "Status")
    private String Status;

    @JsonProperty(value = "ExistingJobStatus")
    private String ExistingJobStatus;

    @JsonProperty(value = "Message")
    private String Message;

    @JsonProperty(value = "NumberTotalRows")
    private long NumberTotalRows;

    @JsonProperty(value = "NumberLoadedRows")
    private long NumberLoadedRows;

    @JsonProperty(value = "NumberFilteredRows")
    private int NumberFilteredRows;

    @JsonProperty(value = "NumberUnselectedRows")
    private int NumberUnselectedRows;

    @JsonProperty(value = "LoadBytes")
    private long LoadBytes;

    @JsonProperty(value = "LoadTimeMs")
    private int LoadTimeMs;

    @JsonProperty(value = "BeginTxnTimeMs")
    private int BeginTxnTimeMs;

    @JsonProperty(value = "StreamLoadPutTimeMs")
    private int StreamLoadPutTimeMs;

    @JsonProperty(value = "ReadDataTimeMs")
    private int ReadDataTimeMs;

    @JsonProperty(value = "WriteDataTimeMs")
    private int WriteDataTimeMs;

    @JsonProperty(value = "CommitAndPublishTimeMs")
    private int CommitAndPublishTimeMs;

    @JsonProperty(value = "ErrorURL")
    private String ErrorURL;

    public long getTxnId() {
        return TxnId;
    }

    public String getStatus() {
        return Status;
    }

    public String getMessage() {
        return Message;
    }

    public String getErrorURL() {
        return ErrorURL;
    }

    public String getLabel() {
        return Label;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "";
        }

    }

    public boolean isSuccess() {
        return DORIS_SUCCESS_STATUS.contains(getStatus());
    }

    public boolean isCopyIntoSuccess(){
        return this.msg.equals("success");
    }
}