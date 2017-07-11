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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildBase;

import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexIdWithVertex;

import java.util.Set;
import java.util.TreeSet;

/**
 * Aggregates the grouping values and concatenates the labels in one super vertex group and
 * assigns this to the super vertex group item.
 */
//@FunctionAnnotation.ForwardedFields("sourceId->f0;targetId->f1;")
//@FunctionAnnotation.ReadFields("label;properties")
public class UpdateSuperVertexGroupItem
  extends BuildBase
  implements CoGroupFunction<SuperVertexGroupItem, SuperVertexIdWithVertex, SuperVertexGroupItem> {

  /**
   * Avoid object instantiation.
   */
  private Set<GradoopId> usedVertices;
  /**
   * Avoid object instantiation.
   */
  private StringBuilder label;
  /**
   * Avoid object instantiation.
   */
  private TreeSet<SuperVertexIdWithVertex> orderedSet;

  /**
   * Creates map function.
   *
   * @param useLabel true, if vertex label shall be used
   */
  public UpdateSuperVertexGroupItem(boolean useLabel) {
    super(useLabel);
    label = new StringBuilder();
    usedVertices = Sets.newHashSet();
    orderedSet = Sets.newTreeSet();
  }

  @Override
  public void coGroup(
    Iterable<SuperVertexGroupItem> superVertexGroupItems,
    Iterable<SuperVertexIdWithVertex> superVertexIdWithVertices,
    Collector<SuperVertexGroupItem> collector)
    throws Exception {

    usedVertices.clear();
    orderedSet.clear();
    label.setLength(0);
    boolean isFirst = true;
    Vertex vertex;
    // the super vertex group items are all the same and only the first is needed
    SuperVertexGroupItem superVertexGroupItem = superVertexGroupItems.iterator().next();
    // natural ordered set
    for (SuperVertexIdWithVertex superVertexIdWithVertex : superVertexIdWithVertices) {
      orderedSet.add(superVertexIdWithVertex);
    }

    for (SuperVertexIdWithVertex superVertexIdWithVertex : orderedSet) {
      vertex = superVertexIdWithVertex.getVertex();

      // build the new super vertex label
      if (useLabel()) {
        if (!isFirst) {
          label.append(Grouping.LABEL_SEPARATOR);
        }
        label.append(vertex.getLabel());
      }
      // aggregate the values of each vertex which is assigned to the super vertex
      if (doAggregate(superVertexGroupItem.getLabelGroup().getAggregators())) {
        aggregate(
          getAggregateValues(vertex, superVertexGroupItem.getLabelGroup().getAggregators()),
          superVertexGroupItem.getLabelGroup().getAggregators());
      }
      if (isFirst) {
        superVertexGroupItem.setGroupingValues(
          getGroupProperties(vertex, superVertexGroupItem.getLabelGroup().getPropertyKeys()));
        isFirst = false;
      }
      usedVertices.add(vertex.getId());
    }
    superVertexGroupItem.setGroupLabel(label.toString());
    superVertexGroupItem.setAggregateValues(
      getAggregateValues(superVertexGroupItem.getLabelGroup().getAggregators()));

    collector.collect(superVertexGroupItem);
    resetAggregators(superVertexGroupItem.getLabelGroup().getAggregators());
  }
}
