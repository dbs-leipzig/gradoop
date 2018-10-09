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
package org.gradoop.flink.algorithms.gelly.pagerank;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * A test for {@link PageRank}, calling the operator and checking if the result was set.
 */
public class PageRankTest extends GradoopFlinkTestBase {

  /**
   * The property key used to store the page rank result.
   */
  public static final String PROPERTY_KEY = "pageRank";

  /**
   * Execute the {@link PageRank} operator and check if the property was set for all vertices.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testPageRankExecution() throws Exception {
    LogicalGraph input = getSocialNetworkLoader().getLogicalGraphByVariable("g0");
    long inputVertexCount = input.getVertices().count();
    LogicalGraph result = new PageRank(PROPERTY_KEY, 0.3, 20).execute(input);
    List<Vertex> resultVertices = result.getVertices().collect();
    assertEquals(inputVertexCount, resultVertices.size());
    for (Vertex vertex : resultVertices) {
      assertTrue(vertex.hasProperty(PROPERTY_KEY));
    }
  }
}