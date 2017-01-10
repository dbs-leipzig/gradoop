package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.TransactionalFSMBase;
import org.gradoop.flink.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
/**
 * Base class for Transactional Frequent Subgraph Mining with Generator Tests.
 */
@RunWith(Parameterized.class)
public abstract class TransactionalFSMWithGeneratorTestBase extends GradoopFlinkTestBase {

  private final String testName;

  private final boolean directed;

  private final float threshold;

  private final long graphCount;

  public TransactionalFSMWithGeneratorTestBase(String testName, String directed,
    String threshold, String graphCount) {
    this.testName = testName;
    this.directed = Boolean.parseBoolean(directed);
    this.threshold = Float.parseFloat(threshold);
    this.graphCount = Long.parseLong(graphCount);
  }

  public abstract TransactionalFSMBase getImplementation(FSMConfig config);

  @Test
  public void withGeneratorTest() throws Exception {
    FSMConfig config = new FSMConfig(threshold, directed);

    GraphTransactions transactions = new PredictableTransactionsGenerator(
      graphCount, 1, true, getConfig()).execute();

    DataSet<GraphTransaction> frequentSubgraphs = getImplementation(config)
      .execute(transactions)
      .getTransactions();

    if (directed){
      Assert.assertEquals(PredictableTransactionsGenerator
        .containedDirectedFrequentSubgraphs(threshold), frequentSubgraphs.count());
    } else {
      Assert.assertEquals(PredictableTransactionsGenerator
        .containedUndirectedFrequentSubgraphs(threshold), frequentSubgraphs.count());
    }
  }
}
