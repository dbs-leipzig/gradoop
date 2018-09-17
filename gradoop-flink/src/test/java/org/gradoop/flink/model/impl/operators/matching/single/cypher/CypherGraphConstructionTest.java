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
package org.gradoop.flink.model.impl.operators.matching.single.cypher;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class CypherGraphConstructionTest extends GradoopFlinkTestBase {

  @Test
  public void testEdgeConstruction() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph dbGraph = loader.getLogicalGraph();

    loader.appendToDatabaseFromString("expected0[" +
        "(alice)-[akb]->(bob)-[bkc]->(carol)," +
        "(alice)-[:possible_friend]->(carol)" +
      "]," +
      "expected1[" +
        "(bob)-[bkc]->(carol)-[ckd]->(dave)," +
        "(bob)-[:possible_friend]->(dave)" +
      "]");

    GraphCollection result = dbGraph.query(
      "MATCH (a:Person)-[e0:knows]->(b:Person)-[e1:knows]->(c:Person) " +
        "WHERE a.city = 'Leipzig' AND a <> c",
      "(b)<-[e0]-(a)-[e_new:possible_friend]->(c)<-[e1]-(b)");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("expected0", "expected1");

    collectAndAssertTrue(result.equalsByGraphElementData(expectedCollection));
  }

  @Test
  public void testEdgeConstructionReducedPattern() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph dbGraph = loader.getLogicalGraph();

    loader.appendToDatabaseFromString("expected0[" +
        "(alice)-[:possible_friend]->(carol)" +
      "]," +
      "expected1[" +
        "(bob)-[:possible_friend]->(dave)" +
      "]");

    GraphCollection result = dbGraph.query(
      "MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person) " +
        "WHERE a.city = 'Leipzig' AND a <> c",
      "(a)-[e_new:possible_friend]->(c)");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("expected0", "expected1");

    collectAndAssertTrue(result.equalsByGraphElementData(expectedCollection));
  }

  @Test
  public void testEdgeConstructionExtendedPattern() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph dbGraph = loader.getLogicalGraph();

    loader.appendToDatabaseFromString("expected0[" +
            "(alice)-[:possible_friend]->(:possible_person)-[:possible_friend]->(carol)" +
            "]," +
            "expected1[" +
            "(bob)-[:possible_friend]->(:possible_person)-[:possible_friend]->(dave)" +
            "]");

    GraphCollection result = dbGraph.query(
            "MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person) " +
                    "WHERE a.city = 'Leipzig' AND a <> c",
            "(a)-[e_new:possible_friend]->(v_new:possible_person)-[e_new2:possible_friend]->(c)");

    GraphCollection expectedCollection = loader
            .getGraphCollectionByVariables("expected0", "expected1");

    collectAndAssertTrue(result.equalsByGraphElementData(expectedCollection));
  }
}
