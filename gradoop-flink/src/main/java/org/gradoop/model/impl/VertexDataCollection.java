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
import org.gradoop.model.operators.VertexDataCollectionOperators;

import java.util.Collection;

/**
 * Represents a distributed collection of vertex data. Abstract from a Flink
 * dataset containing Gelly vertices.
 *
 * @param <VD> vertex data type
 */
public class VertexDataCollection<VD extends VertexData> implements
  VertexDataCollectionOperators<VD> {

  /**
   * Flink dataset holding the actual vertex data.
   */
  private DataSet<Vertex<Long, VD>> vertices;

  /**
   * Creates a new vertex collection from a given dataset.
   *
   * @param vertices Gelly vertex dataset
   */
  VertexDataCollection(DataSet<Vertex<Long, VD>> vertices) {
    this.vertices = vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexDataCollection<VD> filter(
    final Predicate<VD> predicateFunction) {
    return new VertexDataCollection<>(
      vertices.filter(new FilterFunction<Vertex<Long, VD>>() {
        @Override
        public boolean filter(Vertex<Long, VD> v) throws Exception {
          return predicateFunction.filter(v.getValue());
        }
      }));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> Iterable<T> values(Class<T> propertyType, String propertyKey) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<VD> collect() throws Exception {
    return vertices.map(new MapFunction<Vertex<Long, VD>, VD>() {
      @Override
      public VD map(Vertex<Long, VD> v) throws Exception {
        return v.getValue();
      }
    }).collect();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long size() throws Exception {
    return vertices.count();
  }
}
