/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.statistics;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Integration test class of {@link MaxDegree}.
 */
public class MaxDegreeTest extends GradoopFlinkTestBase {

  /**
   * Tests the computation of the maximum degree of a logical graph.
   *
   * @throws Exception If loading of the example-graph fails
   */
  @Test
  public void testMaxDegree() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraph();

    long maxDegree = graph.callForGraph(new MaxDegree())
      .getGraphHead()
      .collect()
      .get(0)
      .getPropertyValue(MaxDegree.PROPERTY_MAX_DEGREE).getLong();

    assertEquals(6L, maxDegree);
  }
}
