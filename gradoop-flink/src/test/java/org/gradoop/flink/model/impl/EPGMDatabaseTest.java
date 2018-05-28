/**
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
package org.gradoop.flink.model.impl;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.bool.Equals;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

public class EPGMDatabaseTest extends GradoopFlinkTestBase {

  @Test
  public void testGetExistingGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    String graphVariable = "g0";

    GraphHead g = loader.getGraphHeadByVariable(graphVariable);
    LogicalGraph graphFromLoader = loader.getLogicalGraphByVariable(graphVariable);

    LogicalGraph graphFromDB = loader.getDatabase().getGraph(g.getId());

    // head <> head
    collectAndAssertTrue(graphFromLoader.getGraphHead()
      .cross(graphFromDB.getGraphHead())
      .with(new Equals<>())
    );

    // elements <> elements
    collectAndAssertTrue(graphFromLoader.equalsByElementIds(graphFromDB));
  }

  @Test
  public void testNonExistingGraph() throws Exception {
    collectAndAssertTrue(getSocialNetworkLoader().getDatabase()
      .getGraph(GradoopId.get()).isEmpty());
  }

  @Test
  public void testGetGraphs() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    String[] graphVariables = new String[]{"g0", "g1", "g2"};
    List<GradoopId> graphIds = Lists.newArrayList();

    for(String graphVariable : graphVariables) {
      graphIds.add(loader.getGraphHeadByVariable(graphVariable).getId());
    }

    GradoopIdSet graphIdSet = GradoopIdSet.fromExisting(graphIds);

    GraphCollection collectionFromLoader =
      loader.getGraphCollectionByVariables(graphVariables);

    GraphCollection collectionFromDbViaArray =
      loader.getDatabase().getCollection().getGraphs(graphIdSet);

    GraphCollection collectionFromDbViaSet =
      loader.getDatabase().getCollection().getGraphs(graphIdSet);

    // heads <> heads
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphIds(collectionFromDbViaArray));
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphIds(collectionFromDbViaSet));

    // elements <> elements
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphElementIds(collectionFromDbViaArray));
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphElementIds(collectionFromDbViaSet));
  }

  @Test
  public void testGetCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection collectionFromLoader =
      loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    GraphCollection collectionFromDb = loader.getDatabase().getCollection();

    // heads <> heads
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphIds(collectionFromDb));

    // elements <> elements
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphElementIds(collectionFromDb));
  }
}