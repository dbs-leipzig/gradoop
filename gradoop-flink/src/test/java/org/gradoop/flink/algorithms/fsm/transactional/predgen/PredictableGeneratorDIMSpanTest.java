package org.gradoop.flink.algorithms.fsm.transactional.predgen;

import org.gradoop.flink.algorithms.fsm.TransactionalFSM;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;

/**
 * Creates an {@link TransactionalFSM} instance for test cases
 */
public class PredictableGeneratorDIMSpanTest extends PredictableGeneratorFSMTestBase {

  public PredictableGeneratorDIMSpanTest(String testName, String directed,
    String threshold, String graphCount){
    super(testName, directed, threshold, graphCount);
  }

  @Override
  public UnaryCollectionToCollectionOperator getImplementation(float minSupport, boolean directed) {
    return new TransactionalFSM(new DIMSpanConfig(minSupport, directed)) {
    };
  }

}
