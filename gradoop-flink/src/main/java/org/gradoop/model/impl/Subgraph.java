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

package org.gradoop.model.impl;

import org.apache.flink.api.java.tuple.Tuple2;

public class Subgraph<K, V> extends Tuple2<K, V> {

  private static final long serialVersionUID = 42L;

  public Subgraph() {
  }

  public Subgraph(K k, V val) {
    this.f0 = k;
    this.f1 = val;
  }

  public K getId() {
    return this.f0;
  }

  public V getValue() {
    return this.f1;
  }

  public void setId(K id) {
    this.f0 = id;
  }

  public void setValue(V val) {
    this.f1 = val;
  }
}
