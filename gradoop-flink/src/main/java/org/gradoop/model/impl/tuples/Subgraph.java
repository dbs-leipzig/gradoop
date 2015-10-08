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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Convenience class to encapsulate a {@link Tuple2} representing logical
 * graph data. Similar to {@link org.apache.flink.graph.Vertex} and {@link
 * org.apache.flink.graph.Edge} in Gelly.
 *
 * @param <K> key type
 * @param <V> value type
 * @see org.apache.flink.graph.Vertex
 * @see org.apache.flink.graph.Edge
 */
public class Subgraph<K, V> extends Tuple2<K, V> {

  /**
   * Default constructor.
   */
  public Subgraph() {
  }

  /**
   * Creates a subgraph from the given key and value.
   *
   * @param k   subgraph identifier
   * @param val associated value
   */
  public Subgraph(K k, V val) {
    this.f0 = k;
    this.f1 = val;
  }

  /**
   * Returns the identifier of that subgraph.
   *
   * @return subgraph identifier
   */
  public K getId() {
    return this.f0;
  }

  /**
   * Returns the value associated with that subgraph.
   *
   * @return subgraph value
   */
  public V getValue() {
    return this.f1;
  }

  /**
   * Sets the identifier of that subgraph.
   *
   * @param id subgraph identifier
   */
  public void setId(K id) {
    this.f0 = id;
  }

  /**
   * Sets the value of that subgraph.
   *
   * @param val subgraph value
   */
  public void setValue(V val) {
    this.f1 = val;
  }
}
