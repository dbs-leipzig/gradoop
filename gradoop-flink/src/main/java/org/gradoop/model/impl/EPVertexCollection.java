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

public class EPVertexCollection<VD extends VertexData> implements
  EPVertexCollectionOperators<VD> {

  private DataSet<Vertex<Long, VD>> vertices;

  EPVertexCollection(DataSet<Vertex<Long, VD>> vertices) {
    this.vertices = vertices;
  }

  @Override
  public EPVertexCollection<VD> filter(final Predicate<VD> predicateFunction) {
    return new EPVertexCollection<>(
      vertices.filter(new FilterFunction<Vertex<Long, VD>>() {
        @Override
        public boolean filter(Vertex<Long, VD> v) throws Exception {
          return predicateFunction.filter(v.getValue());
        }
      }));
  }

  @Override
  public <T> Iterable<T> values(Class<T> propertyType, String propertyKey) {
    return null;
  }

  @Override
  public Collection<VD> collect() throws Exception {
    return vertices.map(new MapFunction<Vertex<Long, VD>, VD>() {
      @Override
      public VD map(Vertex<Long, VD> v) throws Exception {
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
