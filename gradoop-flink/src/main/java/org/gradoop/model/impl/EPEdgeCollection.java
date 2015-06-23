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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.gradoop.model.EPEdgeData;
import org.gradoop.model.operators.EPEdgeCollectionOperators;
import org.gradoop.model.helper.Predicate;

public class EPEdgeCollection implements EPEdgeCollectionOperators {

  private DataSet<Edge<Long, EPFlinkEdgeData>> edges;

  EPEdgeCollection(DataSet<Edge<Long, EPFlinkEdgeData>> edges) {
    this.edges = edges;
  }

  @Override
  public EPEdgeCollection select(Predicate<EPEdgeData> predicateFunction) {
    return null;
  }

  @Override
  public <T> Iterable<T> values(Class<T> propertyType, String propertyKey) {
    return null;
  }

  @Override
  public long size() throws Exception {
      return edges.count();
  }
}
