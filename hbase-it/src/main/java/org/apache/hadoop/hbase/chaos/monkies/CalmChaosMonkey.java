/**
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

package org.apache.hadoop.hbase.chaos.monkies;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Chaos Monkey that does nothing.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CHAOS)
@InterfaceStability.Evolving
public class CalmChaosMonkey extends ChaosMonkey {
  @Override
  public void start() throws Exception {

  }

  @Override
  public void stop(String why) {

  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public void waitForStop() throws InterruptedException {

  }

  @Override
  public boolean isDestructive() {
    return false;
  }
}
