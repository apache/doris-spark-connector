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

package org.apache.doris.spark.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ListUtils.class);

    public static List<String> getSerializedList(List<Map<Object, Object>> batch) throws JsonProcessingException {
        List<String> result = new ArrayList<>();
        divideAndSerialize(batch, result);
        return result;
    }

    /***
     * recursively splits large collections to normal collection and serializes the collection
     * @param batch
     * @param result
     * @throws JsonProcessingException
     */
    public static void divideAndSerialize(List<Map<Object, Object>> batch, List<String> result) throws JsonProcessingException {
        String serializedResult = (new ObjectMapper()).writeValueAsString(batch);
        // if an error occurred in the batch call to getBytes ,average divide the batch
        try {
            //the "Requested array size exceeds VM limit" exception occurs when the collection is large
            serializedResult.getBytes("UTF-8");
            result.add(serializedResult);
            return;
        } catch (Throwable error) {
            LOG.error("getBytes error:{} ,average divide the collection", error);
        }
        for (List<Map<Object, Object>> avgSubCollection : getAvgSubCollections(batch)) {
            divideAndSerialize(avgSubCollection, result);
        }
    }

    /***
     * average divide the collection
     * @param values
     * @return
     */
    public static List<List<Map<Object, Object>>> getAvgSubCollections(List<Map<Object, Object>> values) {
        return Lists.partition(values, (values.size() + 1) / 2);
    }
}
