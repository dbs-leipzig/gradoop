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

package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildBase;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.vertexcentric.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexIdWithVertex;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Takes an EPGM edge as input and creates a {@link EdgeGroupItem} which
 * contains only necessary information for further processing.
 *
 */
//@FunctionAnnotation.ForwardedFields("sourceId->f0;targetId->f1;")
//@FunctionAnnotation.ReadFields("label;properties")
public class UpdateSuperVertexGroupItem
  extends BuildBase
  implements CoGroupFunction<SuperVertexGroupItem, SuperVertexIdWithVertex, SuperVertexGroupItem> {

  private final String LABEL_SEPARATOR = "_";

  private Set<GradoopId> usedVertices;

  private StringBuilder label;

  private boolean isFirst;

  private boolean duplicate;

  /**
   * Creates map function.
   *
   * @param groupPropertyKeys vertex property key for grouping
   * @param useLabel          true, if vertex label shall be used
   * @param edgeAggregators   aggregate functions for super edges
   */
  public UpdateSuperVertexGroupItem(List<String> groupPropertyKeys,
    boolean useLabel, List<PropertyValueAggregator> edgeAggregators) {
    super(groupPropertyKeys, useLabel, edgeAggregators);
    usedVertices = new HashSet<>();
    label = new StringBuilder();
  }

  @Override
  public void coGroup(
    Iterable<SuperVertexGroupItem> superVertexGroupItems,
    Iterable<SuperVertexIdWithVertex> superVertexIdWithVertices,
    Collector<SuperVertexGroupItem> collector)
    throws Exception {

    usedVertices.clear();
    label.setLength(0);

    SuperVertexGroupItem superVertexGroupItem = superVertexGroupItems.iterator().next();

    isFirst = true;
    Vertex vertex;

    for (SuperVertexIdWithVertex superVertexIdWithVertex : superVertexIdWithVertices) {
      vertex = superVertexIdWithVertex.getVertex();
      duplicate = usedVertices.contains(vertex.getId());
      if (useLabel()) {
        if (!duplicate) {
          if (!isFirst) {
            label.append(LABEL_SEPARATOR);
          }
          label.append(getLabel(vertex));
        }
      }
        if (doAggregate() && !duplicate) {
          aggregate(getAggregateValues(vertex));
        }
      if (isFirst) {
        superVertexGroupItem.setGroupingValues(getGroupProperties(vertex));
        isFirst = false;
      }
      usedVertices.add(vertex.getId());
    }
    superVertexGroupItem.setGroupLabel(label.toString());
    superVertexGroupItem.setAggregateValues(getAggregateValues());

    collector.collect(superVertexGroupItem);

    resetAggregators();

  }
}