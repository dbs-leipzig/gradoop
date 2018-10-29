/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.matching.common.statistics;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * @Ignore annotation can be removed as soon issue #661 is fixed
 * @link https://github.com/dbs-leipzig/gradoop/issues/661
 */
@Ignore
public class GraphStatisticsHDFSReaderTest extends GraphStatisticsTest {

  private static HBaseTestingUtility utility;

  @BeforeClass
  public static void setUp() throws Exception {
    if (utility == null) {
      utility = new HBaseTestingUtility(HBaseConfiguration.create());
      utility.startMiniCluster().waitForActiveAndReadyMaster();
    }

    // copy test resources to HDFS
    Path localPath = new Path(
      URLDecoder.decode(
        GraphStatisticsHDFSReaderTest.class.getResource("/data/json/sna/statistics").getFile(),
        StandardCharsets.UTF_8.name()));
    Path remotePath = new Path("/");
    utility.getTestFileSystem().copyFromLocalFile(localPath, remotePath);

    // read graph statistics from HDFS
    TEST_STATISTICS = GraphStatisticsHDFSReader.read("hdfs:///statistics", utility.getConfiguration());
  }

  /**
   * Stops the test cluster after the test.
   *
   * @throws Exception
   */
  @AfterClass
  public static void tearDown() throws Exception {
    if (utility != null) {
      utility.shutdownMiniCluster();
    }
  }
}
