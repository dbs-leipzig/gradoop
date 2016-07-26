package org.gradoop.datagen.transactions.predictable;

import org.gradoop.model.GradoopFlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.*;

public class PredictableTransactionsGeneratorTest extends GradoopFlinkTestBase {
  @Test
  public void containedFrequentSubgraphs() throws Exception {

    assertEquals(702,
      PredictableTransactionsGenerator.containedDirectedFrequentSubgraphs(1.0f));
    assertEquals(2808,
      PredictableTransactionsGenerator.containedDirectedFrequentSubgraphs(0.7f));
    assertEquals(6318,
      PredictableTransactionsGenerator.containedDirectedFrequentSubgraphs(0.2f));
    assertEquals(7722,
      PredictableTransactionsGenerator.containedDirectedFrequentSubgraphs(0.0f));
  }

}
