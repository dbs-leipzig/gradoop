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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildGroupItemBase;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;

import java.util.List;

/**
 * Creates a minimal representation of edge data to be used for grouping.
 *
 * The output of that mapper is a {@link SuperEdgeGroupItem} that contains
 * the edge id, edge label, edge source, edge target, edge group properties and edge aggregate
 * properties.
 */
//@FunctionAnnotation.ForwardedFields("id->f0")
//@FunctionAnnotation.ReadFields("label;properties") //TODO check for updates (source,target)
public class PrepareSuperEdgeGroupItem
  extends BuildGroupItemBase
  implements FlatMapFunction<Edge, SuperEdgeGroupItem> {

  /**
   * Reduce object instantiations.
   */
  private final SuperEdgeGroupItem reuseSuperEdgeGroupItem;

  /**
   * Creates map function
   *
   * @param useLabel true, if label shall be considered
   * @param labelGroups all edge label groups
   */
  public PrepareSuperEdgeGroupItem(boolean useLabel, List<LabelGroup> labelGroups) {
    super(useLabel, labelGroups);

    this.reuseSuperEdgeGroupItem = new SuperEdgeGroupItem();
    this.reuseSuperEdgeGroupItem.setSuperEdgeId(GradoopId.NULL_VALUE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(Edge edge, Collector<SuperEdgeGroupItem> collector)
    throws Exception {
    boolean usedEdgeLabelGroup = false;

    reuseSuperEdgeGroupItem.setEdgeId(edge.getId());
    reuseSuperEdgeGroupItem.setSourceId(edge.getSourceId());
    reuseSuperEdgeGroupItem.setTargetId(edge.getTargetId());

    // check if edge shall be grouped by a special set of keys
    for (LabelGroup edgeLabelGroup : getLabelGroups()) {
      if (edgeLabelGroup.getGroupingLabel().equals(edge.getLabel())) {
        usedEdgeLabelGroup = true;
        setGroupItem(reuseSuperEdgeGroupItem, edge, edgeLabelGroup);
        collector.collect(reuseSuperEdgeGroupItem);
      }
    }
    // standard grouping case
    if (!usedEdgeLabelGroup) {
      setGroupItem(reuseSuperEdgeGroupItem, edge, getDefaultLabelGroup());
      collector.collect(reuseSuperEdgeGroupItem);
    }
  }
}
