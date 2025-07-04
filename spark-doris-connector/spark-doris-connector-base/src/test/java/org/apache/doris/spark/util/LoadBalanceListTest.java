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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class LoadBalanceListTest {

  @Test
  public void testLoadBalanceList() {
    List<String> serverList = Arrays.asList("server1", "server2", "server3");
    LoadBalanceList<String> loadBalanceList = new LoadBalanceList<>(serverList);
    Set<String> testHeadSet = new HashSet<>();
    for(int i = 0; i < 1000; i++) {
      Set<String> serverSet = new HashSet<>();
      int index = 0;
      for (String server: loadBalanceList) {
        serverSet.add(server);
        if (index++ == 0) {
          testHeadSet.add(server);
        }
        System.out.println(server);
      }
      System.out.println("---------");
      Assert.assertTrue(serverSet.size() == serverList.size());
    }
    Assert.assertTrue(testHeadSet.size() == serverList.size());
  }
}
