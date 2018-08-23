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
package org.gradoop.flink.model.impl.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
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

    LogicalGraph input = loader.getLogicalGraph();

    LogicalGraph expected =
      loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input
      .subgraph(
        v -> v.getLabel().equals("Person"),
        e -> e.getLabel().equals("knows"));

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testExistingSubgraphWithVerification() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave)" +
      "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave)" +
      "(eve)-[eka]->(alice)" +
      "(eve)-[ekb]->(bob)" +
      "(frank)-[fkc]->(carol)" +
      "(frank)-[fkd]->(dave)" +
      "]");

    LogicalGraph input = loader.getLogicalGraph();

    LogicalGraph expected =
      loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input
      .subgraph(
        v -> v.getLabel().equals("Person"),
        e -> e.getLabel().equals("knows"),
        Subgraph.Strategy.BOTH_VERIFIED);

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

    LogicalGraph input = loader.getLogicalGraph();

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

    LogicalGraph input = loader.getLogicalGraph();

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

    LogicalGraph input = loader.getLogicalGraph();

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

    LogicalGraph input = loader.getLogicalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input.edgeInducedSubgraph(
      e -> e.getLabel().equals("hasTag"));

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testEdgeInducedSubgraphProjectFirst() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)" +
      "]");

    LogicalGraph input = loader.getLogicalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input.subgraph(null,
      e -> e.getLabel().equals("hasTag"), Subgraph.Strategy.EDGE_INDUCED_PROJECT_FIRST);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testCollectionSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString(
      "(jay:Person {" +
        "name : \"Jay\", age : 45, gender : \"f\", city : \"Leipzig\"})" +
      "g4:Community[" +
        "(jay)-[jkb:knows {since : 2016}]->(bob)" +
        "(bob)-[blj:likes]->(jay)" +
        "]");

    loader.appendToDatabaseFromString(
      "expected0[" +
        "(alice)" +
        "(bob)" +
        "]"
    );

    loader.appendToDatabaseFromString(
      "expected1[]"
    );

    loader.appendToDatabaseFromString(
      "expected4[" +
        "(jay)-[jkb]->(bob)" +
        "]"
    );

    GraphCollection input = loader.getGraphCollectionByVariables("g0","g1","g4");

    FilterFunction<Vertex> vertexFilterFunction = v -> {
      PropertyValue city = v.getPropertyValue("city");
      return city != null && city.toString().equals("Leipzig");
    };

    FilterFunction<Edge> edgeFilterFunction = e -> {
      if (e.getLabel().equals("knows")) {
        if (e.getPropertyValue("since").getInt() == 2016){
          return true;
        }
      }
      return false;
    };

    GraphCollection result = input
      .apply(new ApplySubgraph(vertexFilterFunction, edgeFilterFunction));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
    collectAndAssertTrue(result.equalsByGraphData(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
  }

  @Test
  public void testCollectionVertexInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString(
      "(jay:Person {" +
        "name : \"Jay\", age : 45, gender : \"f\", city : \"Leipzig\"})" +
      "g4:Community[" +
        "(jay)-[jkb:knows]->(bob)" +
        "(bob)-[blj:likes]->(jay)" +
        "]");

    loader.appendToDatabaseFromString(
      "expected0[" +
        "(alice)-[akb]->(bob)-[bka]->(alice)" +
        "]"
    );

    loader.appendToDatabaseFromString(
      "expected1[]"
    );

    loader.appendToDatabaseFromString(
      "expected4[" +
        "(jay)-[jkb]->(bob)-[blj]->(jay)" +
        "]"
    );

    GraphCollection input = loader.getGraphCollectionByVariables("g0","g1","g4");

    FilterFunction<Vertex> vertexFilterFunction = v -> {
      PropertyValue city = v.getPropertyValue("city");
      return city != null && city.toString().equals("Leipzig");
    };

    GraphCollection result = input
      .apply(new ApplySubgraph(vertexFilterFunction, null));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
    collectAndAssertTrue(result.equalsByGraphData(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
  }

  @Test
  public void testCollectionEdgeInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString(
      "expected0[" +
        "(eve)-[ekb]->(bob)" +
        "]"
    );

    loader.appendToDatabaseFromString(
      "expected1[" +
        "(frank)-[fkc]->(carol)" +
        "(frank)-[fkd]->(dave)" +
        "]"
    );

    loader.appendToDatabaseFromString(
      "expected2[]"
    );

    GraphCollection input = loader.getGraphCollectionByVariables("g0","g1","g2");

    GraphCollection result = input
      .apply(new ApplySubgraph(null, e -> e.getPropertyValue("since").getInt() == 2015));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected2")));
    collectAndAssertTrue(result.equalsByGraphData(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected2")));
  }

  @Test
  public void testKeepOnlyRelevantVertices() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("source:G {source : \"graph\"}[" +
        "      (a:Patent {author : \"asdf\", year: 2000, title: \"P1\"})-[:cite {difference : 0}]->(b:Patent {author : \"asdf\", year: 2000, title: \"P2\"})" +
        "      (a)-[:cite {difference : 0}]->(c:Patent {author : \"asdf\", year: 2000, title: \"P3\"})" +
        "      (b)-[:cite {difference : 0}]->(c)\n" +
        "      (a)-[:cite {difference : 5}]->(d:Patent {author : \"zxcv\", year: 1995, title: \"Earlier...\"})" +
        "      (b)-[:cite {difference : 5}]->(d)" +
        "      (e:Patent {author : \"kdkdkd\", year: 1997, title: \"Once upon a time\"})-[e_d:cite {difference : 2}]->(d)" +
        "]");
    GraphCollection sourceGraph = loader.getGraphCollectionByVariables("source");
    // Caution: We can't use result.equalsByGraphElementIds because it internally uses a cross join
    // with equality of elements, which means, it ignores elements that are not within the other dataset
    // This means, the test would succeed even though we have too many vertices as a result of the
    // subgraph operator.
    org.junit.Assert.assertEquals(3, sourceGraph
        .apply(new ApplySubgraph(null, edge -> edge.getPropertyValue("difference").getInt() == 0))
        .getVertices()
        .collect().size());
  }
}
