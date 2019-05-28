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
package org.gradoop.flink.model.impl.operators.verify;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for the {@link VerifyGraphContainment} and {@link VerifyGraphsContainment} operators.
 */
public class VerifyGraphContainmentTest extends GradoopFlinkTestBase {

  /**
   * Test VerifyGraphContainment on a logical graph
   *
   * @throws Exception on failure
   */
  @Test
  public void testOnGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph graph = loader.getLogicalGraphByVariable("g1");

    GradoopIdSet idSet = GradoopIdSet.fromExisting(graph.getGraphHead().map(new Id<>()).collect());

    graph = graph.verifyGraphContainment();

    assertGraphContainment(idSet, graph.getVertices());
    assertGraphContainment(idSet, graph.getEdges());
  }

  /**
   * Test VerifyGraphsContainment on a graph collection
   *
   * @throws Exception on failure
   */
  @Test
  public void testOnCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection collection = loader.getGraphCollectionByVariables("g1", "g2");

    GradoopIdSet idSet = GradoopIdSet.fromExisting(collection.getGraphHeads().map(new Id<>()).collect());

    collection = collection.verifyGraphsContainment();

    assertGraphContainment(idSet, collection.getVertices());
    assertGraphContainment(idSet, collection.getEdges());
  }

  /**
   * Function to test graph element datasets for dangling graph ids.
   *
   * @param idSet ids of the corresponding graph or graph collection
   * @param elements element dataset to test
   * @param <E> element type
   * @throws Exception on failure
   */
  private <E extends EPGMGraphElement> void assertGraphContainment(GradoopIdSet idSet, DataSet<E> elements)
    throws Exception {
    for (E element : elements.collect()) {
      GradoopIdSet ids = element.getGraphIds();
      assertFalse("Element has no graph ids", ids.isEmpty());
      ids.removeAll(idSet);
      assertTrue("There were dangling graph ids", ids.isEmpty());
    }
  }
}
