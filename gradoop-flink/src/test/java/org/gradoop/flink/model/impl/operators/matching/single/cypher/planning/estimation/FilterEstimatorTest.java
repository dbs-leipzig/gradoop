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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import com.google.common.collect.Sets;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectEdgesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectVerticesNode;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FilterEstimatorTest extends EstimatorTestBase {

  @Test
  public void testVertex() throws Exception {
    String query = "MATCH (n)";
    QueryHandler queryHandler = new QueryHandler(query);

    FilterAndProjectVerticesNode node = new FilterAndProjectVerticesNode(null,
      "n", queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());

    FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS);
    elementEstimator.visit(node);

    assertThat(elementEstimator.getCardinality(), is(11L));
    assertThat(elementEstimator.getSelectivity(), is(1d));
  }

  @Test
  public void testVertexWithLabel() throws Exception {
    String query = "MATCH (n:Tag)";
    QueryHandler queryHandler = new QueryHandler(query);

    FilterAndProjectVerticesNode node = new FilterAndProjectVerticesNode(null,
      "n", queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());

    FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS);
    elementEstimator.visit(node);

    assertThat(elementEstimator.getCardinality(), is(3L));
    assertThat(elementEstimator.getSelectivity(), is(1d));
  }

  @Test
  public void testEdge() throws Exception {
    String query = "MATCH (n)-[e]->(m)";
    QueryHandler queryHandler = new QueryHandler(query);

    FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS);
    elementEstimator.visit(node);

    assertThat(elementEstimator.getCardinality(), is(24L));
    assertThat(elementEstimator.getSelectivity(), is(1d));
  }

  @Test
  public void testEdgeWithLabel() throws Exception {
    String query = "MATCH (n)-[e:knows]->(m)";
    QueryHandler queryHandler = new QueryHandler(query);

    FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS);
    elementEstimator.visit(node);

    assertThat(elementEstimator.getCardinality(), is(10L));
    assertThat(elementEstimator.getSelectivity(), is(1d));
  }
}