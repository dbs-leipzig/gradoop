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

import static org.junit.Assert.assertTrue;

/**
 * Test-class for graph density computation.
 */
public class GraphDensityTest extends GradoopFlinkTestBase {

  /**
   * Tests the computation of graph density for a logical graph
   *
   * @throws Exception If loading of the example-graph fails
   */
  @Test
  public void testGraphDensity() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraph();

    double density = graph.callForGraph(new GraphDensity())
      .getGraphHead()
      .collect()
      .get(0)
      .getPropertyValue(SamplingEvaluationConstants.PROPERTY_KEY_DENSITY).getDouble();

    // density should not be 0
    assertTrue("Graph density is 0", density > 0.);
    // density for social network graph should be (24 / 11 * 10) = 0.21818...
    assertTrue("Computed graph density is incorrect", density == (24d / 110d));
  }
}
