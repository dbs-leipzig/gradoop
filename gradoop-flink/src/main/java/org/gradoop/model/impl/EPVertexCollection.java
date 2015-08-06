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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.operators.EPVertexCollectionOperators;

import java.util.Collection;

public class EPVertexCollection implements
  EPVertexCollectionOperators<VertexData> {

  private DataSet<Vertex<Long, VertexData>> vertices;

  EPVertexCollection(DataSet<Vertex<Long, VertexData>> vertices) {
    this.vertices = vertices;
  }

  @Override
  public EPVertexCollection filter(
    final Predicate<VertexData> predicateFunction) {
    return new EPVertexCollection(
      vertices.filter(new FilterFunction<Vertex<Long, VertexData>>() {
        @Override
        public boolean filter(Vertex<Long, VertexData> v) throws Exception {
          return predicateFunction.filter(v.getValue());
        }
      }));
  }

  @Override
  public <V> Iterable<V> values(Class<V> propertyType, String propertyKey) {
    return null;
  }

  @Override
  public Collection<VertexData> collect() throws Exception {
    return vertices
      .map(new MapFunction<Vertex<Long, VertexData>, VertexData>() {
        @Override
        public VertexData map(Vertex<Long, VertexData> v) throws Exception {
          return v.getValue();
        }
      }).collect();
  }

  @Override
  public long size() throws Exception {
    return vertices.count();
  }

  @Override
  public void print() throws Exception {
    vertices.print();
  }
}
