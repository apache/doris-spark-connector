package org.apache.doris.common;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

@Data
public class ResponseEntity {

    private Integer code;
    private String msg;
    private JsonNode data;
    private Integer count;

}
