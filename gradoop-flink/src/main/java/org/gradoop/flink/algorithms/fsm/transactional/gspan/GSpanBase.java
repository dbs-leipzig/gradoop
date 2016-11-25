package org.gradoop.flink.algorithms.fsm.transactional.gspan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.gradoop.flink.algorithms.fsm.transactional.TransactionalFSMBase;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.DirectedGSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.UndirectedGSpanKernel;
import org.gradoop.flink.algorithms.fsm_old.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm_old.common.functions.Frequent;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.ToAdjacencyList;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.ToGraphTransaction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.Validate;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.util.GradoopFlinkConfig;

public abstract class GSpanBase extends TransactionalFSMBase {
  protected final GSpanKernel gSpan;

  public GSpanBase(FSMConfig fsmConfig) {
    super(fsmConfig);
    gSpan = fsmConfig.isDirected() ? new DirectedGSpanKernel() : new UndirectedGSpanKernel();
  }

  /**
   * Encodes the search space before executing FSM.
   *
   * @param input search space as Gradoop graph transactions
   *
   * @return frequent subgraphs as Gradoop graph transactions
   */
  @Override
  public DataSet<GraphTransaction> execute(GraphTransactions input) {

    DataSet<GraphTransaction> transactions = preProcess(input);
    GradoopFlinkConfig config = input.getConfig();

    DataSet<AdjacencyList<LabelPair>> adjacencyLists = transactions
      .map(new ToAdjacencyList());

    // from here

    DataSet<TraversalCode<String>> allFrequentPatterns = mine(adjacencyLists, config);

    // end

    return allFrequentPatterns
      .map(new ToGraphTransaction());
  }

  protected abstract DataSet<TraversalCode<String>> mine(
    DataSet<AdjacencyList<LabelPair>> graphs, GradoopFlinkConfig config);

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
