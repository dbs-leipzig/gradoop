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

package org.gradoop.flink.model.impl.operators.matching.preserving.explorative.tuples;

import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Represents a vertex that is joined with an {@link EmbeddingWithTiePoint}
 * to extend it at the tie point.
 *
 * @param <K> key type
 */
public class VertexStep<K> extends Tuple1<K> {

  public K getVertexId() {
    return f0;
  }

  public void setVertexId(K vertexId) {
    f0 = vertexId;
  }
}
