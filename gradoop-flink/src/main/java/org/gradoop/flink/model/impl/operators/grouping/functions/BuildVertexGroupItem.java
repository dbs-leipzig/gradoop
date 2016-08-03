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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.common.model.impl.properties.PropertyValueList;

import java.util.List;

/**
 * Creates a minimal representation of vertex data to be used for grouping.
 *
 * The output of that mapper is {@link VertexGroupItem} that contains
 * the vertex id, vertex label, vertex group properties and vertex aggregate
 * properties.
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("label;properties")
public class BuildVertexGroupItem extends BuildBase implements
  MapFunction<Vertex, VertexGroupItem> {

  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * Creates map function
   *
   * @param groupPropertyKeys vertex property keys
   * @param useLabel          true, if label shall be considered
   * @param vertexAggregators aggregate functions for super vertices
   */
  public BuildVertexGroupItem(List<String> groupPropertyKeys,
    boolean useLabel, List<PropertyValueAggregator> vertexAggregators) {
    super(groupPropertyKeys, useLabel, vertexAggregators);

    this.reuseVertexGroupItem = new VertexGroupItem();
    this.reuseVertexGroupItem.setSuperVertexId(GradoopId.NULL_VALUE);
    this.reuseVertexGroupItem.setSuperVertex(false);
    if (!doAggregate()) {
      this.reuseVertexGroupItem.setAggregateValues(
        PropertyValueList.createEmptyList());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexGroupItem map(Vertex vertex) throws Exception {
    reuseVertexGroupItem.setVertexId(vertex.getId());
    reuseVertexGroupItem.setGroupLabel(getLabel(vertex));
    reuseVertexGroupItem.setGroupingValues(getGroupProperties(vertex));
    if (doAggregate()) {
      reuseVertexGroupItem.setAggregateValues(getAggregateValues(vertex));
    }
    return reuseVertexGroupItem;
  }
}
