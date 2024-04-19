package org.apache.doris.common;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class DppResult implements Serializable {

    @JsonProperty(value = "is_success", required = true)
    public boolean isSuccess;

    @JsonProperty(value = "failed_reason", required = true)
    public String failedReason;

    @JsonProperty(value = "scanned_rows", required = true)
    public long scannedRows;

    @JsonProperty(value = "file_number", required = true)
    public long fileNumber;

    @JsonProperty(value = "file_size", required = true)
    public long fileSize;

    @JsonProperty(value = "normal_rows", required = true)
    public long normalRows;

    @JsonProperty(value = "abnormal_rows", required = true)
    public long abnormalRows;

    @JsonProperty(value = "unselect_rows", required = true)
    public long unselectRows;

    // only part of abnormal rows will be returned
    @JsonProperty("partial_abnormal_rows")
    public String partialAbnormalRows;

    @JsonProperty("scanned_bytes")
    public long scannedBytes;

    public DppResult() {
        isSuccess = true;
        failedReason = "";
        scannedRows = 0;
        fileNumber = 0;
        fileSize = 0;
        normalRows = 0;
        abnormalRows = 0;
        unselectRows = 0;
        partialAbnormalRows = "";
        scannedBytes = 0;
    }


}