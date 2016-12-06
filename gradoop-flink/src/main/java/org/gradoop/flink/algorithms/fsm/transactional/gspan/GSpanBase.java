package org.gradoop.flink.algorithms.fsm.transactional.gspan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.ToUndirectedAdjacencyList;
import org.gradoop.flink.algorithms.fsm.transactional.tle.TransactionalFSMBase;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.DirectedGSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.UndirectedGSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.common.Constants;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.Frequent;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.ToDirectedAdjacencyList;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.ToGraphTransaction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.Validate;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.model.impl.tuples.WithCount;

import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

public abstract class GSpanBase extends TransactionalFSMBase {
  protected final GSpanKernel gSpan;

  public GSpanBase(FSMConfig fsmConfig) {
    super(fsmConfig);
    gSpan = fsmConfig.isDirected() ? new DirectedGSpanKernel() : new UndirectedGSpanKernel();
  }

  /**
   * Encodes the search space before executing FSM.
   *
   * @param transactions search space as Gradoop graph transactions
   *
   * @return frequent subgraphs as Gradoop graph transactions
   */
  @Override
  public DataSet<GraphTransaction> execute(DataSet<GraphTransaction> transactions) {

    transactions = preProcess(transactions);

    DataSet<AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>> adjacencyLists =
      transactions
        .map(fsmConfig.isDirected() ?
          new ToDirectedAdjacencyList() : new ToUndirectedAdjacencyList());

    DataSet<TraversalCode<String>> allFrequentPatterns = mine(adjacencyLists);

    return allFrequentPatterns
      .map(new ToGraphTransaction());
  }

  protected abstract DataSet<TraversalCode<String>> mine(DataSet<AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>> graphs);

  protected DataSet<TraversalCode<String>> getFrequentPatterns(
    FlatMapOperator<GraphEmbeddingsPair, WithCount<TraversalCode<String>>> reports) {
    return reports
        .groupBy(0)
        .combineGroup(sumPartition())
        .filter(new Validate(gSpan))
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<>())
        .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
        .map(new ValueOfWithCount<>());
  }
}
