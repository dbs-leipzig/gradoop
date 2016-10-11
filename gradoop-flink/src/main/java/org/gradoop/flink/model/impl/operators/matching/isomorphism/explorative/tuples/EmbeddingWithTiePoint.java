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
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;

/**
 * Represents an embedding and a weld point to grow the embedding.
 *
 * f0: tie point
 * f1: embedding
 *
 * @param <K> key type
 */
public class EmbeddingWithTiePoint<K>
  extends Tuple2<K, Embedding<K>> {

  public K getTiePointId() {
    return f0;
  }

  public void setTiePointId(K tiePointId) {
    f0 = tiePointId;
  }

  public Embedding<K> getEmbedding() {
    return f1;
  }

  public void setEmbedding(Embedding<K> embedding) {
    f1 = embedding;
  }
}
