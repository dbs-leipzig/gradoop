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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildGroupItemBase;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;

import java.util.List;

/**
 * Returns tuples which assigns each source/target set of gradoop ids the edge id.
 */
public class PrepareSuperVertexGroupItem
  extends BuildGroupItemBase
  implements FlatMapFunction<SuperEdgeGroupItem, SuperVertexGroupItem> {

  /**
   * Avoid object initialization in each call.
   */
  private SuperVertexGroupItem reuseSuperVertexGroupItem;

  /**
   * Constructor to initialize object.
   *
   * @param useLabel true, if vertex label shall be considered
   * @param labelGroups all vertex label groups
   */
  public PrepareSuperVertexGroupItem(boolean useLabel, List<LabelGroup> labelGroups) {
    super(useLabel, labelGroups);
    reuseSuperVertexGroupItem = new SuperVertexGroupItem();
    reuseSuperVertexGroupItem.setSuperVertexId(GradoopId.NULL_VALUE);
    reuseSuperVertexGroupItem.setGroupingValues(PropertyValueList.createEmptyList());
    reuseSuperVertexGroupItem.setAggregateValues(PropertyValueList.createEmptyList());
    reuseSuperVertexGroupItem.setLabelGroup(getDefaultLabelGroup());

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(SuperEdgeGroupItem superEdgeGroupItem,
    Collector<SuperVertexGroupItem> collector) throws Exception {

    reuseSuperVertexGroupItem.setVertexIds(superEdgeGroupItem.getSourceIds());
    reuseSuperVertexGroupItem.setSuperEdgeId(superEdgeGroupItem.getEdgeId());
    collector.collect(reuseSuperVertexGroupItem);

    reuseSuperVertexGroupItem.setVertexIds(superEdgeGroupItem.getTargetIds());
    reuseSuperVertexGroupItem.setSuperEdgeId(superEdgeGroupItem.getEdgeId());
    collector.collect(reuseSuperVertexGroupItem);
  }
}
