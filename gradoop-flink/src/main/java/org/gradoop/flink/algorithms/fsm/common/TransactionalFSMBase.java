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

package org.gradoop.flink.algorithms.fsm.common;

import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;


/**
 * Superclass of transactional FSM and derivatives.
 */
public abstract class TransactionalFSMBase
  implements UnaryCollectionToCollectionOperator {

  /**
   * frequent subgraph mining configuration
   */
  protected final FSMConfig fsmConfig;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public TransactionalFSMBase(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  /**
   * Calculates the number of iterations necessary to fine all frequent
   * subgraphs according to a specified maximum edge count.
   *
   * @return number of iterations
   */
  protected int getMaxIterations() {
    return (int) (Math.log(fsmConfig.getMaxEdgeCount() - 1) / Math.log(2));
  }
}
