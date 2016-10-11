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

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Represents an edge that is joined with an {@link EmbeddingWithTiePoint} to
 * extend it at the tie point.
 *
 * f0: edge id
 * f1: tie point (sourceId/targetId)
 * f2: next id (sourceId/targetId)
 *
 * @param <K> key type
 */
public class EdgeStep<K> extends Tuple3<K, K, K> {

  public K getEdgeId() {
    return f0;
  }

  public void setEdgeId(K edgeId) {
    f0 = edgeId;
  }

  public K getTiePoint() {
    return f1;
  }

  public void setTiePointId(K tiePoint) {
    f1 = tiePoint;
  }

  public K getNextId() {
    return f2;
  }

  public void setNextId(K nextId) {
    f2 = nextId;
  }
}
