package org.apache.doris.common.meta;

import org.apache.doris.common.LoadInfo;

import lombok.Data;

@Data
public class LoadInfoResponse {

    private String status;
    private String msg;
    private LoadInfo jobInfo;

}
