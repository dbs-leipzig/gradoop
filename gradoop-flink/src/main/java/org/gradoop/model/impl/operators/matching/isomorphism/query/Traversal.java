package org.gradoop.model.impl.operators.matching.isomorphism.query;

import java.util.List;

/**
 * Interface representing a step by step traversal over a graph.
 */
public interface Traversal {
  /**
   * Return a list containing each step of the traversal.
   * @return traversal steps
   */
  List<Step> getSteps();

  Step getStep(int i);
}
