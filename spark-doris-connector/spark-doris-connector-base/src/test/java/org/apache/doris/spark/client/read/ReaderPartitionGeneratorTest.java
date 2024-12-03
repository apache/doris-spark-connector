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

package org.apache.doris.spark.client.read;

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.QueryPlan;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.rest.models.Tablet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReaderPartitionGeneratorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testMappingBeToTablets() {

        Map<String, Tablet> partitions = new HashMap<>();
        partitions.put("1", new Tablet(Arrays.asList("be01:9030", "be02:9030", "be03:9030"), 2, 1, 1));
        partitions.put("2", new Tablet(Arrays.asList("be01:9030", "be02:9030", "be03:9030"), 2, 1, 1));
        partitions.put("3", new Tablet(Arrays.asList("be01:9030", "be02:9030", "be03:9030"), 2, 1, 1));
        partitions.put("4", new Tablet(Arrays.asList("be01:9030", "be02:9030", "be03:9030"), 2, 1, 1));
        partitions.put("5", new Tablet(Arrays.asList("be01:9030", "be02:9030", "be03:9030"), 2, 1, 1));
        partitions.put("6", new Tablet(Arrays.asList("be01:9030", "be02:9030", "be03:9030"), 2, 1, 1));
        partitions.put("7", new Tablet(Arrays.asList("be01:9030", "be02:9030", "be03:9030"), 2, 1, 1));
        partitions.put("8", new Tablet(Arrays.asList("be01:9030", "be02:9030", "be03:9030"), 2, 1, 1));
        partitions.put("9", new Tablet(Arrays.asList("be01:9030", "be02:9030", "be03:9030"), 2, 1, 1));
        partitions.put("10", new Tablet(Arrays.asList("be01:9030", "be02:9030", "be03:9030"), 2, 1, 1));

        QueryPlan queryPlan = new QueryPlan();
        queryPlan.setStatus(0);
        queryPlan.setOpaqued_query_plan("");
        queryPlan.setPartitions(partitions);

        Map<String, List<Long>> result = ReaderPartitionGenerator.mappingBeToTablets(queryPlan);
        Assert.assertTrue(result.size() == 3
                && result.get("be01:9030").size() == 4
                && result.get("be02:9030").size() == 3
                && result.get("be03:9030").size() == 3);

    }

    @Test
    public void getFinalReadColumnsTest(@Mocked DorisConfig config, @Mocked DorisFrontendClient frontendClient) throws Exception {

        String[] cols = new String[]{"c0", "c1", "c2"};
        Schema schema = new Schema();
        schema.setProperties(Arrays.asList(new Field("c0", "int", "", 0, 0, ""),
                new Field("c1", "bitmap", "", 0, 0, ""),
                new Field("c2", "hll", "", 0, 0, "")                ));

        new Expectations() {{
            config.getValue(DorisOptions.DORIS_READ_BITMAP_TO_BASE64);
            result = true;
            config.getValue(DorisOptions.DORIS_READ_BITMAP_TO_STRING);
            result = false;
            frontendClient.getTableSchema(anyString, anyString);
            result = schema;
        }};

        String[] finalReadColumns1 = ReaderPartitionGenerator.getFinalReadColumns(config, frontendClient, "db", "tbl", cols);
        Assert.assertArrayEquals(new String[]{"`c0`", "bitmap_to_base64(`c1`) AS `c1`", "'Read unsupported' AS `c2`"}, finalReadColumns1);

        new Expectations() {{
            config.getValue(DorisOptions.DORIS_READ_BITMAP_TO_BASE64);
            result = false;
            config.getValue(DorisOptions.DORIS_READ_BITMAP_TO_STRING);
            result = true;
            frontendClient.getTableSchema(anyString, anyString);
            result = schema;
        }};

        String[] finalReadColumns2 = ReaderPartitionGenerator.getFinalReadColumns(config, frontendClient, "db", "tbl", cols);
        Assert.assertArrayEquals(new String[]{"`c0`", "bitmap_to_string(`c1`) AS `c1`", "'Read unsupported' AS `c2`"}, finalReadColumns2);

    }

}