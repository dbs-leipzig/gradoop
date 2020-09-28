/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import com.google.common.collect.Sets;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalEdgesNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalVerticesNode;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

public class FilterTemporalEstimatorTest extends TemporalGradoopTestBase {

  TemporalGraphStatistics stats;
  CNFEstimation est;

  @Before
  public void setUp() throws Exception {
    stats = new BinningTemporalGraphStatisticsFactory().fromGraph(
      loadCitibikeSample());
  }

  @Test
  public void testVertex() throws Exception {
    String query = "MATCH (n) WHERE n.tx_to.after(Timestamp(2013-07-20))";
    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query,
      new CNFPostProcessing(new ArrayList<>()));
    est = new CNFEstimation(stats, queryHandler);

    FilterAndProjectTemporalVerticesNode node = new FilterAndProjectTemporalVerticesNode(null,
      "n", queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());

    FilterTemporalEstimator elementEstimator = new FilterTemporalEstimator(queryHandler, stats, est);
    elementEstimator.visit(node);

    assertThat(elementEstimator.getCardinality(), is(30L));
    assertThat(elementEstimator.getSelectivity(), is(elementEstimator.getCnfEstimation()
      .estimateCNF(queryHandler.getPredicates().getSubCNF("n"))));
  }

  @Test
  public void testVertexWithLabel() throws Exception {
    String query = "MATCH (n:Tag)";
    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query,
      new CNFPostProcessing(new ArrayList<>()));
    est = new CNFEstimation(stats, queryHandler);

    FilterAndProjectTemporalVerticesNode node = new FilterAndProjectTemporalVerticesNode(null,
      "n", queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());

    FilterTemporalEstimator elementEstimator = new FilterTemporalEstimator(queryHandler, stats, est);
    elementEstimator.visit(node);

    assertThat(elementEstimator.getCardinality(), is(0L));
    // selectivity for any label is 1.
    assertEquals(elementEstimator.getSelectivity(), 1., 0.001);
  }

  @Test
  public void testEdge() throws Exception {
    String query = "MATCH (n)-[e]->(m) WHERE n.val_to.before(m.val_from)";
    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query,
      new CNFPostProcessing(new ArrayList<>()));
    est = new CNFEstimation(stats, queryHandler);

    FilterAndProjectTemporalEdgesNode node = new FilterAndProjectTemporalEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    FilterTemporalEstimator elementEstimator = new FilterTemporalEstimator(queryHandler, stats, est);
    elementEstimator.visit(node);

    assertThat(elementEstimator.getCardinality(), is(20L));
    assertThat(elementEstimator.getSelectivity(), is(
      elementEstimator.getCnfEstimation().estimateCNF(
        queryHandler.getPredicates().getSubCNF("e")
      )));
  }

  @Test
  public void testEdgeWithLabel() throws Exception {
    String query = "MATCH (n)-[e:unknown]->(m)";
    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query,
      new CNFPostProcessing(new ArrayList<>()));
    est = new CNFEstimation(stats, queryHandler);

    FilterAndProjectTemporalEdgesNode node = new FilterAndProjectTemporalEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    FilterTemporalEstimator elementEstimator = new FilterTemporalEstimator(queryHandler, stats, est);
    elementEstimator.visit(node);

    assertThat(elementEstimator.getCardinality(), is(0L));
    // selectivity for any label is 1
    assertEquals(elementEstimator.getSelectivity(), 1., 0.0001);
  }
}
