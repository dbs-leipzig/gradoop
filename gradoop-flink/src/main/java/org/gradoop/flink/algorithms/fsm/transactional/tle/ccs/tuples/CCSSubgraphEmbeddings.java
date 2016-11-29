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

package org.gradoop.flink.algorithms.fsm.transactional.tle.ccs.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.SubgraphEmbeddings;

import java.util.List;

/**
 * Representation of a subgraph supported by a graph and all its local
 * embeddings.
 *
 * (category, graphId, size, canonicalLabel, embeddings)
 */
public class CCSSubgraphEmbeddings
  extends Tuple5<GradoopId, Integer, String, List<Embedding>, String>
  implements SubgraphEmbeddings {

  /**
   * Default constructor
   */
  public CCSSubgraphEmbeddings() {
  }


  public String getCategory() {
    return f4;
  }

  public void setCategory(String category) {
    f4 = category;
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


  public String getCanonicalLabel() {
    return f2;
  }

  public void setCanonicalLabel(String label) {
    f2 = label;
  }

  public List<Embedding> getEmbeddings() {
    return f3;
  }

  public void setEmbeddings(List<Embedding> embeddings) {
    f3 = embeddings;
  }
}
