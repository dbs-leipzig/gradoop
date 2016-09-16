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

package org.gradoop.flink.algorithms.fsm.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.pojos.Embedding;

import java.util.Collection;

/**
 * Representation of a subgraph supported by a graph and all its local
 * embeddings.
 *
 * (graphId, size, canonicalLabel, embeddings)
 */
public class SubgraphEmbeddings
  extends Tuple4<GradoopId, Integer, String, Collection<Embedding>> {

  /**
   * Default constructor
   */
  public SubgraphEmbeddings() {
  }

  public GradoopId getGraphId() {
    return f0;
  }

  public void setGraphId(GradoopId graphId) {
    f0 = graphId;
  }

  public Integer getSize() {
    return f1;
  }

  public void setSize(Integer size) {
    f1 = size;
  }


  public String getSubgraph() {
    return f2;
  }

  public void setSubgraph(String subgraph) {
    f2 = subgraph;
  }

  public Collection<Embedding> getEmbeddings() {
    return f3;
  }

  public void setEmbeddings(Collection<Embedding> embeddings) {
    f3 = embeddings;
  }
}
