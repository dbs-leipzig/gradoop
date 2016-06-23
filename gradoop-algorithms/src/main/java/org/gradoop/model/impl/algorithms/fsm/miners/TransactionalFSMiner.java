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


package org.gradoop.model.impl.algorithms.fsm.miners;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.tuples.WithCount;

/**
 * Describes the core of a transactional FSM implementation.
 *
 * @param <T> graph representation
 */
public interface TransactionalFSMiner<T> {

  /**
   * Triggers the mining process.
   *
   * @param edgeTriples input edge triples
   * @param minFrequency minimum frequency
   * @param fsmConfig FSM configuration
   * @return frequent subgraphs with frequency
   */
  DataSet<WithCount<T>> mine(DataSet<EdgeTriple> edgeTriples,
    DataSet<Integer> minFrequency, FSMConfig fsmConfig);

  /**
   * Sets the Flink execution environment.
   *
   * @param env execution environment
   */
  void setExecutionEnvironment(ExecutionEnvironment env);
}
