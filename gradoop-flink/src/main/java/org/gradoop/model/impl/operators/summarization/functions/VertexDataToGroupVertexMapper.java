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

package org.gradoop.model.impl.operators.summarization.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.operators.summarization.Summarization;
import org.gradoop.model.impl.operators.summarization.tuples.VertexForGrouping;

/**
 * Creates a minimal representation of vertex data to be used for grouping.
 *
 * The output of that mapper is {@link VertexForGrouping} that contains
 * the vertex id, vertex label and vertex property. Depending on the
 * summarization parameters, label and property can be null.
 *
 * @param <VD> vertex data type
 */
@FunctionAnnotation.ForwardedFields("f0")
public class VertexDataToGroupVertexMapper<VD extends VertexData> implements
  MapFunction<Vertex<Long, VD>, VertexForGrouping> {

  /**
   * Vertex property key that can be used for grouping.
   */
  private final String groupPropertyKey;
  /**
   * True, if vertex label shall be considered.
   */
  private final boolean useLabel;
  /**
   * True, if vertex property shall be considered.
   */
  private final boolean useProperty;
  /**
   * Reduce object instantiations.
   */
  private final VertexForGrouping reuseVertexForGrouping;

  /**
   * Creates map function
   *
   * @param groupPropertyKey vertex property key
   * @param useLabel         true, if label shall be considered
   */
  public VertexDataToGroupVertexMapper(String groupPropertyKey,
    boolean useLabel) {
    this.groupPropertyKey = groupPropertyKey;
    this.useLabel = useLabel;
    useProperty = groupPropertyKey != null && !"".equals(groupPropertyKey);
    reuseVertexForGrouping = new VertexForGrouping();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexForGrouping map(Vertex<Long, VD> vertex) throws Exception {
    reuseVertexForGrouping.setVertexId(vertex.getId());
    reuseVertexForGrouping.setGroupLabel(getGroupLabel(vertex.getValue()));
    reuseVertexForGrouping
      .setGroupPropertyValue(getGroupPropertyValue(vertex.getValue()));
    return reuseVertexForGrouping;
  }

  /**
   * Returns the vertex label or {@code null} if no label is required.
   *
   * @param vertexData vertex data
   * @return vertex label or {@code null} if no label is required
   */
  private String getGroupLabel(VD vertexData) {
    return useLabel ? vertexData.getLabel() : null;
  }

  /**
   * Returns the vertex property value. If the vertex property shall be
   * considered in the grouping but the vertex does not have that value,
   * a default value is returned. If grouping on vertex properties is not
   * necessary, null is returned.
   *
   * @param vertexData vertex data
   * @return property value, default value or {@code null}
   */
  private String getGroupPropertyValue(VD vertexData) {
    boolean hasProperty = vertexData.getProperty(groupPropertyKey) != null;
    String returnValue = null;
    if (useProperty && hasProperty) {
      returnValue = vertexData.getProperty(groupPropertyKey).toString();
    } else if (useProperty) {
      returnValue = Summarization.NULL_VALUE;
    }
    return returnValue;
  }
}
