/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.storage.impl.hbase;

import org.gradoop.storage.config.GradoopHBaseConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link HBaseEPGMStore} with pre-split regions.
 */
public class HBaseSplitRegionGraphStoreTest extends HBaseDefaultGraphStoreTest {

  /**
   * Instantiate the EPGMStore with a prefix and persist social media data.
   * In addition, use pre-split regions.
   *
   * @throws IOException on failure
   */
  @BeforeClass
  public static void setUp() throws IOException {
    final GradoopHBaseConfig config = GradoopHBaseConfig.getDefaultConfig();
    config.enablePreSplitRegions(32);
    socialNetworkStore = openEPGMStore("HBaseGraphStoreSplitRegionTest.", config);
    writeSocialGraphToStore(socialNetworkStore);
  }

  @Test
  public void testConfig() {
    assertTrue(socialNetworkStore.getConfig().getVertexHandler().isPreSplitRegions());
    assertTrue(socialNetworkStore.getConfig().getEdgeHandler().isPreSplitRegions());
    assertTrue(socialNetworkStore.getConfig().getGraphHeadHandler().isPreSplitRegions());
  }
}
