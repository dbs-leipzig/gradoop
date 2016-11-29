package org.gradoop.flink.algorithms.fsm.transactional;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.SumAggregationFunction;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.gradoop.flink.algorithms.fsm_old.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm_old.common.functions.Frequent;
import org.gradoop.flink.algorithms.fsm_old.common.functions.MinFrequency;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.*;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.util.GradoopFlinkConfig;

public abstract class TransactionalFSMBase
  implements UnaryCollectionToCollectionOperator {
  protected final FSMConfig fsmConfig;
  /**
   * minimum frequency for subgraphs to be considered to be frequent
   */
  protected DataSet<Long> minFrequency;
  protected GradoopFlinkConfig gradoopFlinkConfig;

  public TransactionalFSMBase(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public GraphCollection execute(GraphCollection collection)  {
    gradoopFlinkConfig = collection.getConfig();

    DataSet<GraphTransaction> input = collection
      .toTransactions()
      .getTransactions();

    DataSet<GraphTransaction> output = execute(input);

    return GraphCollection.fromTransactions(new GraphTransactions(output, gradoopFlinkConfig));
  }

  public GraphTransactions execute(GraphTransactions transactions) {
    this.gradoopFlinkConfig = transactions.getConfig();

    DataSet<GraphTransaction> input = transactions.getTransactions();
    DataSet<GraphTransaction> output = execute(input);

    return new GraphTransactions(output, gradoopFlinkConfig);
  }

  protected abstract DataSet<GraphTransaction> execute(DataSet<GraphTransaction> transactions);

  protected DataSet<GraphTransaction> preProcess(DataSet<GraphTransaction> transactions) {
    transactions = transactions
      .map(new DropPropertiesAndGraphContainment());

    this.minFrequency = Count
      .count(transactions)
      .map(new MinFrequency(fsmConfig));

    DataSet<String> frequentVertexLabels = transactions
      .flatMap(new VertexLabels())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<>());

    transactions = transactions
      .map(new FilterVerticesByLabel())
      .withBroadcastSet(frequentVertexLabels, Constants.FREQUENT_VERTEX_LABELS);

    DataSet<String> frequentEdgeLabels = transactions
      .flatMap(new EdgeLabels())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<>());

    transactions = transactions
      .map(new FilterEdgesByLabel())
      .withBroadcastSet(frequentEdgeLabels, Constants.FREQUENT_EDGE_LABELS);

    transactions = transactions
      .filter(new NotEmpty());

    return transactions;
  }

  protected GroupCombineFunction<WithCount<TraversalCode<String>>, WithCount<TraversalCode<String>>> sumPartition() {

    SumAggregationFunction.LongSumAgg[] sum = { new SumAggregationFunction.LongSumAgg() };

    int[] fields = { 1 };

    return new AggregateOperator.AggregatingUdf(sum, fields);
  }
}
