
package org.gradoop.flink.model.impl.operators.matching.common.query;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * A traversal code represents a traversal through a query graph.
 */
public class TraversalCode implements Serializable {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;
  /**
   * Steps represent the traversal code.
   */
  private final List<Step> steps;

  /**
   * Initialize a new traversal code
   */
  public TraversalCode() {
    this.steps = Lists.newArrayList();
  }

  /**
   * Add a step to the traversal code.
   *
   * @param step new step
   */
  public void add(Step step) {
    steps.add(step);
  }

  /**
   * Return a list containing each step of the traversal.
   *
   * @return traversal steps
   */
  public List<Step> getSteps() {
    return steps;
  }

  /**
   * Return the i-th step of the traversal code.
   *
   * @param i number of step to be returned
   * @return Step at the i-th position
   */
  public Step getStep(int i) {
    return steps.get(i);
  }

  @Override
  public String toString() {
    return "TraversalCode{steps=" + steps + '}';
  }
}
