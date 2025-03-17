package org.apache.doris.spark.client.entity;

public class CopyIntoResponse {

    private Integer code;
    private String msg;
    private String content;

    public CopyIntoResponse(Integer code, String msg, String content) {
        this.code = code;
        this.msg = msg;
        this.content = content;
    }

    public Integer getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    public String getContent() {
        return content;
    }
}
