package org.gradoop.flink.algorithms.fsm2;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.SumAggregationFunction;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.gradoop.flink.algorithms.fsm.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.functions.Frequent;
import org.gradoop.flink.algorithms.fsm2.functions.DropPropertiesAndGraphContainment;
import org.gradoop.flink.algorithms.fsm2.functions.EdgeLabels;
import org.gradoop.flink.algorithms.fsm2.functions.FilterEdgesByLabel;
import org.gradoop.flink.algorithms.fsm2.functions.FilterVerticesByLabel;
import org.gradoop.flink.algorithms.fsm2.functions.VertexLabels;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Created by peet on 23.11.16.
 */
public abstract class TransactionalFSMBase
  implements UnaryCollectionToCollectionOperator {
  protected final FSMConfig fsmConfig;
  /**
   * minimum frequency for subgraphs to be considered to be frequent
   */
  protected DataSet<Long> minFrequency;

  public TransactionalFSMBase(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public GraphCollection execute(GraphCollection collection)  {
    GradoopFlinkConfig gradoopFlinkConfig = collection.getConfig();

    // create transactions from graph collection
    GraphTransactions transactions = collection
      .toTransactions();

    return GraphCollection.fromTransactions(
      new GraphTransactions(execute(transactions), gradoopFlinkConfig));
  }

  protected abstract DataSet<GraphTransaction> execute(GraphTransactions transactions);

  protected DataSet<GraphTransaction> preProcess(GraphTransactions input) {
    DataSet<GraphTransaction> transactions = input
      .getTransactions()
      .map(new DropPropertiesAndGraphContainment());

    this.minFrequency = Count.count(transactions);

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
    return transactions;
  }

  protected GroupCombineFunction<WithCount<TraversalCode<String>>, WithCount<TraversalCode<String>>> sumPartition() {

    SumAggregationFunction.LongSumAgg[] sum = { new SumAggregationFunction.LongSumAgg() };

    int[] fields = { 1 };

    return new AggregateOperator.AggregatingUdf(sum, fields);
  }
}
