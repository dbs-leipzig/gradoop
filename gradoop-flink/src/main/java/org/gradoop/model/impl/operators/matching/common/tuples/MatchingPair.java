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

package org.gradoop.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

/**
 * Represents a vertex and a single incident edge.
 *
 * f0: source vertex
 * f1: outgoing edge
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class MatchingPair<V extends EPGMVertex, E extends EPGMEdge>
  extends Tuple2<V, E> {

  public V getVertex() {
    return f0;
  }

  public void setVertex(V v) {
    f0 = v;
  }

  public E getEdge() {
    return f1;
  }

  public void setEdge(E e) {
    f1 = e;
  }
}
