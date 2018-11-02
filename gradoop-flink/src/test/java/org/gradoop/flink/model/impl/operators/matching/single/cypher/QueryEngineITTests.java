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
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsLocalFSReader;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.planner.greedy.GreedyPlanner;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class QueryEngineITTests extends GradoopFlinkTestBase {

  private LogicalGraph socialNetwork;

  private GraphStatistics socialNetworkStatistics;

  @Before
  public void setUp() throws Exception {
    socialNetwork = getSocialNetworkLoader().getLogicalGraph();
    String path = getFilePath("/data/json/sna/statistics");
    socialNetworkStatistics = GraphStatisticsLocalFSReader.read(path);
  }

  @Test
  public void testMatchVertex() throws Exception {
    assertCardinalities("MATCH (n:Person)", 6, 6);
  }

  @Test
  public void testMatchEdge() throws Exception {
    assertCardinalities("MATCH ()-[:knows]->()", 10, 10);
  }

  @Test
  public void testMatchOneHop() throws Exception {
    assertCardinalities("MATCH (:Person)<--(:Forum)", 5, 6);
  }

  @Test
  public void testMatchTwoHops() throws Exception {
    assertCardinalities("MATCH (:Tag)<--()-->(:Person)", 26, 18);
  }

  @Test
  public void testMatchVariableLengthPath() throws Exception {
    assertCardinalities("MATCH ()-[*0..10]->()", 98, 86);
  }

  @Test
  public void testMatchWithValueJoin() throws Exception {
    assertCardinalities("MATCH (a:Person), (b:Person) WHERE a.city = b.city", 36, 8);
  }

  /**
   * Executed the given query and checks if the estimated and exact cardinality applies to the
   * specified values.
   *
   * @param q cypher query
   * @param estimatedCardinality estimated cardinality of the result
   * @param exactCardinality exact cardinality of the result
   * @throws Exception
   */
  private void assertCardinalities(String q, long estimatedCardinality, int exactCardinality)
    throws Exception {

    final QueryHandler queryHandler = new QueryHandler(q);

    GreedyPlanner planner = new GreedyPlanner(socialNetwork, queryHandler, socialNetworkStatistics,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    PlanTableEntry planTableEntry = planner.plan();
    assertThat(planTableEntry.getEstimatedCardinality(), is(estimatedCardinality));
    List<Embedding> result = planTableEntry.getQueryPlan().execute().collect();
    assertThat(result.size(), is(exactCardinality));
  }
}
