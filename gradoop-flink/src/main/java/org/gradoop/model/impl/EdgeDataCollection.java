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
import org.apache.flink.graph.Edge;
import org.gradoop.model.EdgeData;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.operators.EdgeDataCollectionOperators;

import java.util.Collection;

/**
 * Represents a distributed collection of edge data. Abstracts from a Flink
 * dataset containing Gelly edges.
 *
 * @param <ED> edge data type
 */
public class EdgeDataCollection<ED extends EdgeData> implements
  EdgeDataCollectionOperators<ED> {

  /**
   * Flink dataset holding the actual edge data.
   */
  private DataSet<Edge<Long, ED>> edges;

  /**
   * Creates a new edge collection from a given dataset.
   *
   * @param edges Gelly edge dataset
   */
  EdgeDataCollection(DataSet<Edge<Long, ED>> edges) {
    this.edges = edges;
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
  public long size() throws Exception {
    return edges.count();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeDataCollection<ED> filter(final Predicate<ED> predicateFunction) {
    return new EdgeDataCollection<>(
      edges.filter(new FilterFunction<Edge<Long, ED>>() {
        @Override
        public boolean filter(Edge<Long, ED> e) throws Exception {
          return predicateFunction.filter(e.getValue());
        }
      }));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<ED> collect() throws Exception {
    return edges.map(new MapFunction<Edge<Long, ED>, ED>() {
      @Override
      public ED map(Edge<Long, ED> e) throws Exception {
        return e.getValue();
      }
    }).collect();
  }
}
