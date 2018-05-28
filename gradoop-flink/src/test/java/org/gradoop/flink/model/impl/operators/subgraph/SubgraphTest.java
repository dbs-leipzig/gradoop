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
package org.gradoop.flink.model.impl.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.fusion.VertexFusion;
import org.gradoop.flink.model.impl.operators.transformation.ApplyTransformation;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class SubgraphTest extends GradoopFlinkTestBase {

  @Test
  public void testExistingSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave)" +
      "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave)" +
      "(eve)-[eka]->(alice)" +
      "(eve)-[ekb]->(bob)" +
      "(frank)-[fkc]->(carol)" +
      "(frank)-[fkd]->(dave)" +
      "]");

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    LogicalGraph expected =
      loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input
      .subgraph(
        v -> v.getLabel().equals("Person"),
        e -> e.getLabel().equals("knows"));

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  /**
   * Extracts a subgraph where only vertices fulfill the filter function.
   */
  @Test
  public void testPartialSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice),(bob),(carol),(dave),(eve),(frank)" +
      "]");

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input
      .subgraph(
        v -> v.getLabel().equals("Person"),
        e -> e.getLabel().equals("friendOf"));

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  /**
   * Extracts a subgraph which is empty.
   *
   * @throws Exception
   */
  @Test
  public void testEmptySubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[]");

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input.subgraph(
      v -> v.getLabel().equals("User"),
      e -> e.getLabel().equals("friendOf"));

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testVertexInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)" +
      "]");

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input.vertexInducedSubgraph(
      v -> v.getLabel().equals("Forum") || v.getLabel().equals("Tag"));

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testEdgeInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)" +
      "]");

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input.edgeInducedSubgraph(
      e -> e.getLabel().equals("hasTag"));

    collectAndAssertTrue(output.equalsByElementData(expected));
  }
}
