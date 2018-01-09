/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
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
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("label;properties;sourceId;targetId")
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
