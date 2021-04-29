/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@Category(LargeTests.class)
public class TestCompactionOffloadSwitch {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionOffloadSwitch.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCompactionOffloadSwitch.class);
  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration conf = HBaseConfiguration.create();
  private static Connection connection;
  private static Admin admin;

  @Parameterized.Parameter
  public boolean compactionOffloadSwitch;

  @Parameterized.Parameters
  public static Collection<Boolean> parameters() {
    return new ArrayList<>(Arrays.asList(true, false));
  }

  @Before
  public void setUp() throws Exception {
    conf.setBoolean(HConstants.COMPACTION_OFFLOAD_ENABLED, compactionOffloadSwitch);
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getAdmin();
    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @After
  public void tearDown() throws Exception {
    if (admin != null) {
      admin.close();
    }
    if (connection != null) {
      connection.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCompactionOffloadSwitch() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    boolean previous = compactionOffloadSwitch;
    for (int time = 0; time < 5; time++) {
      boolean setValue = Math.random() < 0.5;
      Map<ServerName, Boolean> map = admin.compactionOffloadSwitch(setValue, new ArrayList<>(0));
      Assert.assertEquals(1 + TEST_UTIL.getHBaseCluster().getRegionServerThreads().size(),
        map.size());
      for (Map.Entry<ServerName, Boolean> entry : map.entrySet()) {
        Assert.assertEquals(previous, entry.getValue());
      }
      previous = setValue && compactionOffloadSwitch;
    }
  }
}
