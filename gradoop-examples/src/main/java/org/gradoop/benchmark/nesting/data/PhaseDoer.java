package org.gradoop.benchmark.nesting.data;

/**
 * Created by vasistas on 14/04/17.
 */
public interface PhaseDoer {

  void loadOperands();
  void performOperation();

  void finalizeLoadOperand();
  void finalizeOperation();

}
