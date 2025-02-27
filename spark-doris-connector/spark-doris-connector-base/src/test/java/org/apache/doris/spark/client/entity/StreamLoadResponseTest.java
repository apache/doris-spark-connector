package org.apache.doris.spark.client.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class StreamLoadResponseTest {

    private final static ObjectMapper MAPPER = JsonMapper.builder().build();

    @Test
    public void testIsSuccess() throws JsonProcessingException {

        String entity1 = "{" +
                "\"TxnId\":1," +
                "\"Label\":\"xxx\"," +
                "\"Status\":\"Success\"," +
                "\"ExistingJobStatus\":\"\"," +
                "\"Message\":\"OK\"," +
                "\"NumberTotalRows\":1," +
                "\"NumberLoadedRows\":1," +
                "\"NumberFilteredRows\":1," +
                "\"NumberUnselectedRows\":1," +
                "\"LoadBytes\":1," +
                "\"LoadTimeMs\":1," +
                "\"BeginTxnTimeMs\":1," +
                "\"StreamLoadPutTimeMs\":1" +
                "}";
        StreamLoadResponse slr1 = MAPPER.readValue(entity1, StreamLoadResponse.class);
        Assertions.assertTrue(slr1.isSuccess());

        String entity2 = "{" +
                "\"TxnId\":1," +
                "\"Label\":\"label\"," +
                "\"Status\":\"Publish Timeout\"," +
                "\"ExistingJobStatus\":\"\"," +
                "\"Message\":\"\"," +
                "\"NumberTotalRows\":1," +
                "\"NumberLoadedRows\":1," +
                "\"NumberFilteredRows\":1," +
                "\"NumberUnselectedRows\":1," +
                "\"LoadBytes\":1," +
                "\"LoadTimeMs\":1," +
                "\"BeginTxnTimeMs\":1," +
                "\"StreamLoadPutTimeMs\":1" +
                "}";
        StreamLoadResponse slr2 = MAPPER.readValue(entity2, StreamLoadResponse.class);
        Assertions.assertTrue(slr2.isSuccess());

        String entity3 = "{" +
                "\"TxnId\":1," +
                "\"Label\":\"xxx\"," +
                "\"Status\":\"Label Already Exists\"," +
                "\"ExistingJobStatus\":\"\"," +
                "\"Message\":\"\"," +
                "\"NumberTotalRows\":1," +
                "\"NumberLoadedRows\":1," +
                "\"NumberFilteredRows\":1," +
                "\"NumberUnselectedRows\":1," +
                "\"LoadBytes\":1," +
                "\"LoadTimeMs\":1," +
                "\"BeginTxnTimeMs\":1," +
                "\"StreamLoadPutTimeMs\":1" +
                "}";
        StreamLoadResponse slr3 = MAPPER.readValue(entity3, StreamLoadResponse.class);
        Assertions.assertFalse(slr3.isSuccess());

        String entity4 = "{" +
                "\"msg\":\"TStatus: errCode = 2, detailMessage = transaction [123] is already aborted, abort reason: User Abort\"," +
                "\"status\":\"ANALYSIS_ERROR\"" +
                "}";
        StreamLoadResponse slr4 = MAPPER.readValue(entity4, StreamLoadResponse.class);
        Assertions.assertFalse(slr4.isSuccess());

    }


}