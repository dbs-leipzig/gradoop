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

package org.gradoop.model.impl.operators.aggregation.functions.count;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.AggregateFunction;
import org.gradoop.model.api.functions.ApplyAggregateFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.ToPropertyValue;
import org.gradoop.model.impl.functions.graphcontainment.ExpandGraphsToIds;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.aggregation.functions
  .GroupCountToPropertyValue;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Aggregate function returning the edge count of a graph / collection.
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class EdgeCount
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements
  AggregateFunction<G, V, E>, ApplyAggregateFunction<G, V, E> {

  /**
   * Returns a 1-element dataset containing the edge count of the given graph.
   *
   * @param graph input graph
   * @return 1-element dataset with vertex count
   */
  @Override
  public DataSet<PropertyValue> execute(LogicalGraph<G, V, E> graph) {
    return Count
      .count(graph.getEdges())
      .map(new ToPropertyValue<Long>());
  }

  /**
   * Returns a dataset containing graph identifiers and the corresponding edge
   * count.
   *
   * @param collection input graph collection
   * @return dataset with graph + edge count tuples
   */
  @Override
  public DataSet<Tuple2<GradoopId, PropertyValue>> execute(
    GraphCollection<G, V, E> collection) {
    return Count.groupBy(
      collection
        .getEdges()
          .flatMap(new ExpandGraphsToIds<E>())
    ).map(new GroupCountToPropertyValue());
  }

  /**
   * Return default property value, in this case 0L.
   * @return default property value of this aggregation function
   */
  @Override
  public Number getDefaultValue() {
    return 0L;
  }
}
