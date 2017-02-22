package org.gradoop.flink.algorithms.fsm;

import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DataflowStep;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DictionaryType;
import org.gradoop.flink.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.junit.Assert;
import org.junit.Test;

public class DIMSpanConfigTest extends GradoopFlinkTestBase {
  private static final float MIN_SUPPORT = 1.0f;
  private static final long GRAPH_COUNT = 10;

  @Test
  public void setDictionaryType() throws Exception {
    for (DictionaryType type : DictionaryType.values()) {
      DIMSpanConfig config = new DIMSpanConfig(MIN_SUPPORT, true);
      if (type != config.getDictionaryType()) {
        config.setDictionaryType(type);
        executeWith(config);
      }
    }
  }

  @Test
  public void setEmbeddingCompressionEnabled() throws Exception {
    DIMSpanConfig config = new DIMSpanConfig(MIN_SUPPORT, true);
    config.setEmbeddingCompressionEnabled(! config.isEmbeddingCompressionEnabled());
    executeWith(config);
  }

  @Test
  public void setGraphCompressionEnabled() throws Exception {
    DIMSpanConfig config = new DIMSpanConfig(MIN_SUPPORT, true);
    config.setGraphCompressionEnabled(! config.isGraphCompressionEnabled());
    executeWith(config);
  }

  @Test
  public void getPatternCompressionInStep() throws Exception {
    for (DataflowStep step : DataflowStep.values()) {
      DIMSpanConfig config = new DIMSpanConfig(MIN_SUPPORT, true);
      if (step != config.getPatternCompressionInStep()) {
        config.setPatternCompressionInStep(step);
        executeWith(config);
      }
    }
  }

  @Test
  public void setPatternValidationInStep() throws Exception {
    for (DataflowStep step : DataflowStep.values()) {
      DIMSpanConfig config = new DIMSpanConfig(MIN_SUPPORT, true);
      if (step != config.getPatternValidationInStep() && step != DataflowStep.WITHOUT) {
        config.setPatternValidationInStep(step);
        executeWith(config);
      }
    }
  }

  @Test
  public void setBranchFilterEnabled() throws Exception {
    DIMSpanConfig config = new DIMSpanConfig(MIN_SUPPORT, true);
    config.setBranchFilterEnabled(! config.isBranchFilterEnabled());
    executeWith(config);
  }

  private void executeWith(DIMSpanConfig config) throws Exception {
    GraphTransactions transactions = new PredictableTransactionsGenerator(
      GRAPH_COUNT, 1, true, getConfig()).execute();

    config.setDirected(true);

    GraphCollection frequentSubgraphs = new TransactionalFSM(config)
      .execute(GraphCollection.fromTransactions(transactions));

    Assert.assertEquals(PredictableTransactionsGenerator
        .containedDirectedFrequentSubgraphs(MIN_SUPPORT),
      frequentSubgraphs.getGraphHeads().count());

    config.setDirected(false);

    frequentSubgraphs = new TransactionalFSM(config)
      .execute(GraphCollection.fromTransactions(transactions));

    Assert.assertEquals(PredictableTransactionsGenerator
        .containedUndirectedFrequentSubgraphs(MIN_SUPPORT),
      frequentSubgraphs.getGraphHeads().count());
  }
}