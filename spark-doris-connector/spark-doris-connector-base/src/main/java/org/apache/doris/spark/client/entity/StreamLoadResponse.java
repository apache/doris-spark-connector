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