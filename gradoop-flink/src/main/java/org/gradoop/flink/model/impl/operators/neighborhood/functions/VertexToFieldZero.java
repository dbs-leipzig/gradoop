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

package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Puts the vertex to the first field of the tuple.
 *
 * @param <K> type of the first field of the tuple
 * @param <V> type of the second field of the tuple
 */
public class VertexToFieldZero<K, V>
  implements JoinFunction<Tuple2<K, V>, Vertex, Tuple2<Vertex, V>> {

  /**
   * Avoid object instantiation.
   */
  private Tuple2<Vertex, V> reuseTuple = new Tuple2<Vertex, V>();

  @Override
  public Tuple2<Vertex, V> join(Tuple2<K, V> tuple, Vertex vertex) throws Exception {
    reuseTuple.setFields(vertex, tuple.f1);
    return reuseTuple;
  }
}

