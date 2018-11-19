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
package org.gradoop.flink.model.impl.tpgm;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class of {@link TemporalGraphFactory}.
 */
public class TemporalGraphFactoryTest extends GradoopFlinkTestBase {

  /**
   * Test if method {@link TemporalGraphFactory#createEmptyGraph()} creates an empty temporal
   * graph instance.
   *
   * @throws Exception if counting the elements fails
   */
  @Test
  public void testCreateEmptyGraph() throws Exception {
    TemporalGraph temporalGraph = getConfig().getTemporalGraphFactory().createEmptyGraph();
    assertEquals(0, temporalGraph.getGraphHead().count());
    assertEquals(0, temporalGraph.getVertices().count());
    assertEquals(0, temporalGraph.getEdges().count());
  }
}
