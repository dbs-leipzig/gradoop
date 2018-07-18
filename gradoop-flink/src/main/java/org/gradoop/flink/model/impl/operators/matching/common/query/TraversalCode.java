/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
