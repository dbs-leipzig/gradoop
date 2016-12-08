/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.transactional;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.GSpanBase;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.CreateCollector;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.ExpandResult;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.HasEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.IsCollector;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.PatternGrowth;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.InitSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.Report;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class GSpanIterative extends GSpanBase {

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   *
   */
  public GSpanIterative(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  protected DataSet<WithCount<TraversalCode<String>>> mine(
    DataSet<AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>> graphs) {

    DataSet<GraphEmbeddingsPair> searchSpace = graphs
      .map(new InitSingleEdgeEmbeddings(gSpan));

    DataSet<GraphEmbeddingsPair> collector = graphs
      .getExecutionEnvironment()
      .fromElements(true)
      .map(new CreateCollector());

    searchSpace = searchSpace.union(collector);

    // ITERATION HEAD

    IterativeDataSet<GraphEmbeddingsPair> iterative = searchSpace
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY

    FlatMapOperator<GraphEmbeddingsPair, WithCount<TraversalCode<String>>> reports = iterative
      .flatMap(new Report());

    DataSet<WithCount<TraversalCode<String>>> frequentPatterns = getFrequentPatterns(reports);

    DataSet<GraphEmbeddingsPair> grownEmbeddings = iterative
      .map(new PatternGrowth(gSpan))
      .withBroadcastSet(frequentPatterns, TFSMConstants.FREQUENT_PATTERNS)
      .filter(new HasEmbeddings());

    // ITERATION FOOTER

    return iterative
      .closeWith(grownEmbeddings, frequentPatterns)
      .filter(new IsCollector())
      .flatMap(new ExpandResult());
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

}
