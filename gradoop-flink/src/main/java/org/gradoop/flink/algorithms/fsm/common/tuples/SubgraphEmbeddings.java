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

package org.gradoop.flink.algorithms.fsm.common.tuples;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.common.pojos.Embedding;

import java.util.Collection;

/**
 * Describes a subgraph and its embeddings in a certain graph.
 */
public interface SubgraphEmbeddings {

  /**
   * Getter.
   *
   * @return graph id
   */
  GradoopId getGraphId();

  /**
   * Setter.
   *
   * @param graphId graph id
   */
  void setGraphId(GradoopId graphId);

  /**
   * Getter.
   *
   * @return edge count
   */
  Integer getSize();

  /**
   * Setter.
   *
   * @param size edge count
   */
  void setSize(Integer size);

  /**
   * Getter.
   *
   * @return canonical label
   */
  String getCanonicalLabel();

  /**
   * Setter.
   *
   * @param label canonical label
   */
  void setCanonicalLabel(String label);

  /**
   * Getter.
   *
   * @return embeddings
   */
  Collection<Embedding> getEmbeddings();

  /**
   * Setter.
   *
   * @param embeddings embeddings
   */
  void setEmbeddings(Collection<Embedding> embeddings);
}
