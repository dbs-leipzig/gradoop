/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
  TraversalCode() {
    this.steps = Lists.newArrayList();
  }

  /**
   * Add a step to the traversal code.
   *
   * @param step new step
   */
  void add(Step step) {
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
}
