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

package org.gradoop.flink.algorithms.fsm.transactional.tle.fsm.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.transactional.tle.common.tuples.SubgraphEmbeddings;

import java.util.List;

/**
 * Representation of a subgraph supported by a graph and all its local
 * embeddings.
 *
 * (graphId, size, canonicalLabel, embeddings)
 */
public class TFSMSubgraphEmbeddings
  extends Tuple4<GradoopId, Integer, String, List<Embedding>>
  implements SubgraphEmbeddings {

  /**
   * Default constructor
   */
  public TFSMSubgraphEmbeddings() {
    super();
  }

  @Override
  public GradoopId getGraphId() {
    return f0;
  }

  @Override
  public void setGraphId(GradoopId graphId) {
    f0 = graphId;
  }

  @Override
  public Integer getSize() {
    return f1;
  }

  @Override
  public void setSize(Integer size) {
    f1 = size;
  }

  @Override
  public String getCanonicalLabel() {
    return f2;
  }

  @Override
  public void setCanonicalLabel(String label) {
    f2 = label;
  }

  @Override
  public List<Embedding> getEmbeddings() {
    return f3;
  }

  @Override
  public void setEmbeddings(List<Embedding> embeddings) {
    f3 = embeddings;
  }
}
