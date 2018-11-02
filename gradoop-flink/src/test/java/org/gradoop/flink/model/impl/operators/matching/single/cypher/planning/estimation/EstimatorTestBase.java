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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsLocalFSReader;
import org.junit.BeforeClass;

public abstract class EstimatorTestBase {

  static GraphStatistics STATS;

  @BeforeClass
  public static void setUp() throws Exception {
    String path = URLDecoder.decode(
      JoinEstimatorTest.class.getResource("/data/json/sna/statistics").getFile(),
      StandardCharsets.UTF_8.name());
    STATS = GraphStatisticsLocalFSReader.read(path);
  }
}
