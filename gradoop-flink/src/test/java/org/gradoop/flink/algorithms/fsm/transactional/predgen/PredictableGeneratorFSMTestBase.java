package org.gradoop.flink.algorithms.fsm.transactional.predgen;

import org.gradoop.flink.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

/**
 * Base class for Transactional Frequent Subgraph Mining with Generator Tests.
 */
@RunWith(Parameterized.class)
public abstract class PredictableGeneratorFSMTestBase extends GradoopFlinkTestBase {

  private final String testName;

  private final boolean directed;

  private final float threshold;

  private final long graphCount;

  public PredictableGeneratorFSMTestBase(String testName, String directed,
    String threshold, String graphCount) {
    this.testName = testName;
    this.directed = Boolean.parseBoolean(directed);
    this.threshold = Float.parseFloat(threshold);
    this.graphCount = Long.parseLong(graphCount);
  }

  public abstract UnaryCollectionToCollectionOperator getImplementation(
    float minSupport, boolean directed);

  @Parameterized.Parameters(name = "{index} : {0}")
  public static Iterable data(){
    return Arrays.asList(
      new String[] {
        "Directed_1.0_10",
        "true",
        "1.0",
        "10"
      },
      new String[] {
        "Directed_0.8_10",
        "true",
        "0.8",
        "10"
      },
      new String[] {
        "Directed_0.6_10",
        "true",
        "0.6",
        "10"
      },
      new String[] {
        "Undirected_1.0_10",
        "false",
        "1.0f",
        "10"
      },
      new String[] {
        "Undirected_0.8_10",
        "false",
        "0.8f",
        "10"
      },
      new String[] {
        "Undirected_0.6_10",
        "false",
        "0.6f",
        "10"
      }
    );
  }

  @Test
  public void withGeneratorTest() throws Exception {
    GraphTransactions transactions = new PredictableTransactionsGenerator(
      graphCount, 1, true, getConfig()).execute();

    GraphCollection frequentSubgraphs = getImplementation(threshold, directed)
      .execute(GraphCollection.fromTransactions(transactions));

    if (directed){
      Assert.assertEquals(PredictableTransactionsGenerator
        .containedDirectedFrequentSubgraphs(threshold), frequentSubgraphs.getGraphHeads().count());
    } else {
      Assert.assertEquals(PredictableTransactionsGenerator
        .containedUndirectedFrequentSubgraphs(threshold), frequentSubgraphs.getGraphHeads().count());
    }
  }

}
