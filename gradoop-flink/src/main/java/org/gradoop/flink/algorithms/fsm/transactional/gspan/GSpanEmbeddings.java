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

package org.gradoop.flink.algorithms.fsm.transactional.gspan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.EmptyGraphEmbeddingPair;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.ExpandResult;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.HasEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.IsCollector;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.PatternGrowth;
import org.gradoop.flink.algorithms.fsm_old.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.InitSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.Report;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingPair;
import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class GSpanEmbeddings extends GSpanBase {

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   *
   */
  public GSpanEmbeddings(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  protected DataSet<TraversalCode<String>> mine(DataSet<AdjacencyList<LabelPair>> graphs, GradoopFlinkConfig config) {

    DataSet<GraphEmbeddingPair> searchSpace = graphs
      .map(new InitSingleEdgeEmbeddings());

    DataSet<GraphEmbeddingPair> collector = config
      .getExecutionEnvironment()
      .fromElements(true)
      .map(new EmptyGraphEmbeddingPair());

    searchSpace = searchSpace.union(collector);

    // ITERATION HEAD

    IterativeDataSet<GraphEmbeddingPair> iterative = searchSpace
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY

    FlatMapOperator<GraphEmbeddingPair, WithCount<TraversalCode<String>>> reports = iterative
      .flatMap(new Report());

    DataSet<TraversalCode<String>> frequentPatterns = getFrequentPatterns(reports);

    DataSet<GraphEmbeddingPair> grownEmbeddings = iterative
      .map(new PatternGrowth())
      .withBroadcastSet(frequentPatterns, Constants.FREQUENT_SUBGRAPHS)
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
