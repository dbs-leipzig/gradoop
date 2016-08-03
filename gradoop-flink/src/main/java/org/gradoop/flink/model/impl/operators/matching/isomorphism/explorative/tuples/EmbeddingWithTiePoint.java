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

package org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;

/**
 * Represents an embedding and a weld point to grow the embedding.
 *
 * f0: embedding
 * f1: tie point
 */
public class EmbeddingWithTiePoint
  extends Tuple2<Embedding, GradoopId> {

  public Embedding getEmbedding() {
    return f0;
  }

  public void setEmbedding(Embedding embedding) {
    f0 = embedding;
  }

  public GradoopId getTiePointId() {
    return f1;
  }

  public void setTiePointId(GradoopId tiePointId) {
    f1 = tiePointId;
  }

}
