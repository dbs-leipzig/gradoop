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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.operators.summarization.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.operators.summarization.tuples.EdgeGroupItem;
import org.gradoop.model.impl.operators.summarization.tuples.VertexWithRepresentative;
import org.gradoop.model.impl.properties.PropertyValue;

import java.util.List;

/**
 * Takes an edge and a tuple (vertex-id, group-representative) as input.
 * Replaces the edge-source-id with the group-representative and outputs
 * projected edge information possibly containing the edge label and a
 * group property.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsSecond("f1") // vertex id -> source id
public class BuildEdgeGroupItem<E extends EPGMEdge>
  extends BuildBase
  implements JoinFunction<E, VertexWithRepresentative, EdgeGroupItem> {

  /**
   * Avoid object initialization in each call.
   */
  private final EdgeGroupItem reuseEdgeGroupItem;

  /**
   * Creates join function.
   *
   * @param groupPropertyKeys vertex property key for grouping
   * @param useLabel          true, if vertex label shall be used
   * @param edgeAggregator    aggregate function for summarized edges
   */
  public BuildEdgeGroupItem(List<String> groupPropertyKeys,
    boolean useLabel, PropertyValueAggregator edgeAggregator) {
    super(groupPropertyKeys, useLabel, edgeAggregator);
    this.reuseEdgeGroupItem = new EdgeGroupItem();
    if (doAggregate() && isCountAggregator()) {
      this.reuseEdgeGroupItem.setGroupAggregate(PropertyValue.create(1L));
    } else {
      this.reuseEdgeGroupItem.setGroupAggregate(PropertyValue.NULL_VALUE);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeGroupItem join(
    E edge, VertexWithRepresentative vertexRepresentative) throws Exception {
    reuseEdgeGroupItem.setSourceId(
      vertexRepresentative.getGroupRepresentative());
    reuseEdgeGroupItem.setTargetId(edge.getTargetId());
    reuseEdgeGroupItem.setGroupLabel(getLabel(edge));
    reuseEdgeGroupItem.setGroupPropertyValues(getGroupProperties(edge));
    if (doAggregate() && !isCountAggregator()) {
      reuseEdgeGroupItem.setGroupAggregate(getValueForAggregation(edge));
    }
    return reuseEdgeGroupItem;
  }
}
