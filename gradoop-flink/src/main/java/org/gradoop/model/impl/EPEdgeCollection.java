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
import org.gradoop.model.operators.EPEdgeCollectionOperators;

import java.util.Collection;

public class EPEdgeCollection implements EPEdgeCollectionOperators<EdgeData> {

  private DataSet<Edge<Long, EdgeData>> edges;

  EPEdgeCollection(DataSet<Edge<Long, EdgeData>> edges) {
    this.edges = edges;
  }

  @Override
  public <T> Iterable<T> values(Class<T> propertyType, String propertyKey) {
    return null;
  }

  @Override
  public long size() throws Exception {
    return edges.count();
  }

  @Override
  public void print() throws Exception {
    edges.print();
  }

  @Override
  public EPEdgeCollection filter(final Predicate<EdgeData> predicateFunction) {
    return new EPEdgeCollection(
      edges.filter(new FilterFunction<Edge<Long, EdgeData>>() {
        @Override
        public boolean filter(Edge<Long, EdgeData> e) throws Exception {
          return predicateFunction.filter(e.getValue());
        }
      }));
  }

  @Override
  public Collection<EdgeData> collect() throws Exception {
    return edges.map(new MapFunction<Edge<Long, EdgeData>, EdgeData>() {
      @Override
      public EdgeData map(Edge<Long, EdgeData> e) throws Exception {
        return e.getValue();
      }
    }).collect();
  }
}
