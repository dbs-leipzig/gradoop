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

package org.gradoop.flink.algorithms.fsm.transactional.tle.common.tuples;

import org.gradoop.flink.algorithms.fsm.transactional.tle.common.pojos.Embedding;
import org.gradoop.flink.model.api.tuples.Countable;

import java.io.Serializable;

/**
 * Describes a subgraph in the context of transactional FSM,
 * i.e., a canonical label and a sample embedding
 */
public interface Subgraph extends Countable, Serializable {

  /**
   * Returns canonical label of a subgraph.
   *
   * @return canonical label
   */
  String getCanonicalLabel();

  /**
   * Sets the canonical label of a subgraph.
   *
   * @param label canonical label
   */
  void setCanonicalLabel(String label);

  /**
   * Returns a sample embedding.
   *
   * @return sample embedding
   */
  Embedding getEmbedding();

  /**
   * Sets the sample embedding.
   *
   * @param embedding sample embedding
   */
  void setEmbedding(Embedding embedding);

}
