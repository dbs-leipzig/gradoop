package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.GSpanEmbeddings;
import org.gradoop.flink.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;
import org.junit.Assert;
import org.junit.Test;

public class PredictableGeneratorCorrectnessTest extends GradoopFlinkTestBase {

  public static final float[] THRESHOLDS = {1.0f, 0.8f, 0.6f};
  private static final long GRAPH_COUNT = 20;

  @Test
  public void testDirected() throws Exception {
    GraphTransactions transactions = new PredictableTransactionsGenerator(
      GRAPH_COUNT, 1, true, getConfig()).execute();

    for (float threshold : THRESHOLDS) {
      mineDirected(transactions, threshold);
    }
  }

  private void mineDirected(
    GraphTransactions transactions, float threshold) throws Exception {

    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    DataSet<GraphTransaction> frequentSubgraphs = new GSpanEmbeddings(fsmConfig)
      .execute(transactions);

    Assert.assertEquals(
      PredictableTransactionsGenerator
        .containedDirectedFrequentSubgraphs(threshold),
      frequentSubgraphs.count()
    );
  }

  @Test
  public void testUndirected() throws Exception {
    GraphTransactions transactions = new PredictableTransactionsGenerator(
      GRAPH_COUNT, 1, true, getConfig()).execute();

    for (float threshold : THRESHOLDS) {
      mineUndirected(transactions, threshold);
    }
  }

  private void mineUndirected(
    GraphTransactions transactions, float threshold) throws Exception {

    FSMConfig fsmConfig = new FSMConfig(threshold, false);

    DataSet<GraphTransaction> frequentSubgraphs = new GSpanEmbeddings(fsmConfig)
      .execute(transactions);

    Assert.assertEquals(
      PredictableTransactionsGenerator
        .containedUndirectedFrequentSubgraphs(threshold),
      frequentSubgraphs.count()
    );
  }

}