package org.apache.doris.common;

import lombok.Data;

import java.util.List;

@Data
public class LoadInfo {

    private String dbName;
    private List<String> tblNames;
    private String label;
    private String clusterName;
    private String state;
    private String failMsg;
    private String trackingUrl;

}