package org.gradoop.flink.algorithms.fsm.transactional.basic;

import org.gradoop.flink.algorithms.fsm.TransactionalFSM;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;

/**
 * Creates an {@link TransactionalFSM} instance for test cases
 */
public class BasicPatternsDIMSpanTest extends BasicPatternsTransactionalFSMTestBase {

  public BasicPatternsDIMSpanTest(String testName, String dataGraph,
    String expectedGraphVariables, String expectedCollection) {
    super(testName, dataGraph, expectedGraphVariables, expectedCollection);
  }

  @Override
  public UnaryCollectionToCollectionOperator getImplementation() {
    return new TransactionalFSM(0.6f);
  }
}
