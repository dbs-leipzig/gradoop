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

package org.gradoop.model.impl.operators.summarization.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.summarization.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.operators.summarization.tuples.VertexGroupItem;
import org.gradoop.model.impl.properties.PropertyValue;

import java.util.List;

/**
 * Creates a minimal representation of vertex data to be used for grouping.
 *
 * The output of that mapper is {@link VertexGroupItem} that contains
 * the vertex id, vertex label, vertex properties and an initial group count.
 *
 * @param <V> EPGM vertex type
 */
public class BuildVertexGroupItem<V extends EPGMVertex>
  extends BuildBase
  implements MapFunction<V, VertexGroupItem> {

  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * Creates map function
   *
   * @param groupPropertyKeys vertex property keys
   * @param useLabel          true, if label shall be considered
   * @param vertexAggregator  aggregate function for summarized vertices
   */
  public BuildVertexGroupItem(List<String> groupPropertyKeys,
    boolean useLabel, PropertyValueAggregator vertexAggregator) {
    super(groupPropertyKeys, useLabel, vertexAggregator);

    this.reuseVertexGroupItem = new VertexGroupItem();
    this.reuseVertexGroupItem.setGroupRepresentative(GradoopId.NULL_VALUE);
    if (doAggregate() && isCountAggregator()) {
      this.reuseVertexGroupItem.setGroupAggregate(PropertyValue.create(1L));
    } else {
      this.reuseVertexGroupItem.setGroupAggregate(PropertyValue.NULL_VALUE);
    }
    this.reuseVertexGroupItem.setCandidate(false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexGroupItem map(V vertex) throws Exception {
    reuseVertexGroupItem.setVertexId(vertex.getId());
    reuseVertexGroupItem.setGroupLabel(getLabel(vertex));
    reuseVertexGroupItem.setGroupPropertyValues(getGroupProperties(vertex));
    if (doAggregate() && !isCountAggregator()) {
      reuseVertexGroupItem.setGroupAggregate(getValueForAggregation(vertex));
    }
    return reuseVertexGroupItem;
  }
}
