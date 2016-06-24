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

package org.gradoop.model.impl.algorithms.fsm.api;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples.EdgeTriple;

import java.util.List;

/**
 * Describes transactional FSM pre processing.
 *
 * @param <T> input type
 */
public interface TransactionalFSMEncoder<T> {

  /**
   * Triggers pre processing
   *
   * @param input input data
   * @param fsmConfig FSM configuration
   * @return edge triples with frequent labels
   */
  DataSet<EdgeTriple> encode(T input, FSMConfig fsmConfig);

  /**
   * Getter.
   *
   * @return FSM threshold frequency
   */
  DataSet<Integer> getMinFrequency();

  /**
   * Getter.
   *
   * @return vertex label dictionary
   */
  DataSet<List<String>> getVertexLabelDictionary();

  /**
   * Getter.
   *
   * @return edge label dictionary
   */
  DataSet<List<String>> getEdgeLabelDictionary();

}
