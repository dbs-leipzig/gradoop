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
package org.gradoop.flink.util;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

import java.util.List;

/**
 * Test class for {@link FlinkAsciiGraphLoader}
 */
public class FlinkAsciiGraphLoaderTest extends GradoopFlinkTestBase {

  /**
   * Test getting a logical graph by variable from the loader
   *
   * @throws Exception on failure
   */
  @Test
  public void testGetLogicalGraphByVariable() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    String graphVariable = "g1";
    GradoopId graphId = loader.getGraphHeadByVariable(graphVariable).getId();

    LogicalGraph graphFromLoader = loader.getLogicalGraphByVariable(graphVariable);
    LogicalGraph graphFromCollection = loader.getGraphCollection().getGraph(graphId);

    collectAndAssertTrue(graphFromLoader.equalsByElementData(graphFromCollection));
  }

  /**
   * Test getting the database graph from the loader.
   * This test will create a loader containing a single graph and check if the database graph
   * is equal to that graph, i.e. if the graph head label was set correctly.
   *
   * @throws Exception if execute fails.
   */
  @Test
  public void testGetLogicalGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("expected:"
      + GradoopConstants.DB_GRAPH_LABEL + "[" +
      "(v1:TestVertex)-[e1:TestEdge]->(v2:TestVertex)" +
      "]");
    LogicalGraph databaseGraph = loader.getLogicalGraph();
    LogicalGraph expectedGraph = loader.getLogicalGraphByVariable("expected");
    collectAndAssertTrue(expectedGraph.equalsByData(databaseGraph));
  }

  /**
   * Test getting a graph collection from the loader
   *
   * @throws Exception on failure
   */
  @Test
  public void testGetGraphCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection collectionFromLoader =
      loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3");
    GraphCollection collectionFromDb = loader.getGraphCollection();

    // heads <> heads
    collectAndAssertTrue(collectionFromLoader.equalsByGraphIds(collectionFromDb));

    // elements <> elements
    collectAndAssertTrue(collectionFromLoader.equalsByGraphElementIds(collectionFromDb));
  }

  /**
   * Test getting a graph collection by variables from the loader
   *
   * @throws Exception on failure
   */
  @Test
  public void testGetGraphCollectionByVariables() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    String[] graphVariables = new String[]{"g0", "g1", "g2"};
    List<GradoopId> graphIds = Lists.newArrayList();

    for(String graphVariable : graphVariables) {
      graphIds.add(loader.getGraphHeadByVariable(graphVariable).getId());
    }

    GradoopIdSet graphIdSet = GradoopIdSet.fromExisting(graphIds);

    GraphCollection collectionFromLoader = loader.getGraphCollectionByVariables(graphVariables);
    GraphCollection collectionFromDbViaSet = loader.getGraphCollection().getGraphs(graphIdSet);

    // heads <> heads
    collectAndAssertTrue(collectionFromLoader.equalsByGraphIds(collectionFromDbViaSet));

    // elements <> elements
    collectAndAssertTrue(collectionFromLoader.equalsByGraphElementIds(collectionFromDbViaSet));
  }
}
