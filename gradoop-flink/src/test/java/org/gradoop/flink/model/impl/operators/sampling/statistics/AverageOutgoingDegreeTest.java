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
package org.gradoop.flink.model.impl.operators.sampling.statistics;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test-class for average outgoing degree computation.
 */
public class AverageOutgoingDegreeTest extends GradoopFlinkTestBase {

  /**
   * Tests the computation of the average outgoing degree for a logical graph
   *
   * @throws Exception If loading of the example-graph fails
   */
  @Test
  public void testAverageOutgoingDegree() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraph();

    long averageOutgoingDegree = graph.callForGraph(new AverageOutgoingDegree())
      .getGraphHead()
      .collect()
      .get(0)
      .getPropertyValue(SamplingEvaluationConstants.PROPERTY_KEY_AVERAGE_OUTGOING_DEGREE).getLong();

    // average outgoing degree for social network graph should be (24 / 11) = 2.1818... -> 3
    assertEquals("Computed average outgoing degree is incorrect", 3L, averageOutgoingDegree);
  }
}
