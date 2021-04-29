/**
 *
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
package org.apache.hadoop.hbase.master;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestVersionUpgrade {
  private static HBaseTestingUtility testingUtility;
  private static LocalHBaseCluster cluster;
  private static final int SLEEP_FOR_BIG_OFFHEAP_CACHE = 20 * 1000;

  @Before
  public void setUp() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniDFSCluster(1);
    testingUtility.startMiniZKCluster(1);
    testingUtility.createRootDir();
    cluster = new LocalHBaseCluster(testingUtility.getConfiguration(), 0, 0);
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    cluster.join();
    testingUtility.shutdownMiniZKCluster();
    testingUtility.shutdownMiniDFSCluster();
  }

  public static class SlowlyInitializingRegionServerWithHigherVersion
    extends MiniHBaseClusterRegionServer {
    private static final AtomicInteger version = new AtomicInteger();

    private static String getTestVersion() {
      return "0.0." + version.getAndIncrement();
    }

    public SlowlyInitializingRegionServerWithHigherVersion(Configuration conf,
        CoordinatedStateManager cp)
      throws IOException, InterruptedException {
      super(conf, cp);
    }

    @Override
    protected void instantiateBlockCache() {
      try {
        super.instantiateBlockCache();
        Thread.sleep(SLEEP_FOR_BIG_OFFHEAP_CACHE);
      } catch (InterruptedException e) {
        // Ignore
      }
    }

    @Override
    protected void createMyEphemeralNode() throws KeeperException, IOException {
      HBaseProtos.RegionServerInfo.Builder rsInfo = HBaseProtos.RegionServerInfo.newBuilder();
      rsInfo.setVersionInfo(ProtobufUtil.getVersionInfo().toBuilder().setVersion(getTestVersion()));
      byte[] data = ProtobufUtil.prependPBMagic(rsInfo.build().toByteArray());
      ZKUtil.createEphemeralNodeAndWatch(zooKeeper, getMyEphemeralNodePath(), data);
    }
  }

  @Test
  public void metaRegionShouldBeMovedIntoRegionServerWithLatestVersionASAP() throws Exception {
    cluster.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    cluster.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 1);
    cluster.getConfiguration().setClass(HConstants.REGION_SERVER_IMPL,
      SlowlyInitializingRegionServerWithHigherVersion.class,
      HRegionServer.class);

    final MasterThread master = cluster.addMaster();
    final RegionServerThread rs1 = cluster.addRegionServer();
    master.start();
    rs1.start();

    waitForClusterOnline(master, 1);

    final RegionServerThread rs2 = cluster.addRegionServer();
    rs2.start();

    waitForClusterOnline(master, 2);

    Waiter.waitFor(cluster.getConfiguration(), 5000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        final RegionStates regionStates =
          master.getMaster().getAssignmentManager().getRegionStates();
        final Set<HRegionInfo> regionsOnRS1 =
          regionStates.getServerRegions(rs1.getRegionServer().getServerName());
        final Set<HRegionInfo> regionsOnRS2 =
          regionStates.getServerRegions(rs2.getRegionServer().getServerName());

        // all system tables should be in rs2.
        return !regionStates.isRegionsInTransition()
          && regionsOnRS1 == null
          && regionsOnRS2 != null && !regionsOnRS2.isEmpty()
          && Iterators.all(regionsOnRS2.iterator(), new Predicate<HRegionInfo>() {
              @Override
              public boolean apply(HRegionInfo hri) {
                return hri.isSystemTable()
                    && regionStates.isRegionInState(hri, RegionState.State.OPEN);
              }
            });
      }
    });
  }

  private static void waitForClusterOnline(MasterThread master, int numRs)
    throws InterruptedException, KeeperException {
    while (!master.getMaster().isInitialized()
            || master.getMaster().getNumLiveRegionServers() != numRs) {
      Thread.sleep(100);
    }
  }
}
