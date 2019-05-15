/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.util.List;

/**
 * Takes an EPGM edge as input and creates a {@link EdgeGroupItem} which
 * contains only necessary information for further processing.
 *
 * @param <E> The edge type.
 */
@FunctionAnnotation.ForwardedFields("sourceId->f0;targetId->f1;")
@FunctionAnnotation.ReadFields("label;properties")
public class BuildEdgeGroupItem<E extends EPGMEdge>
  extends BuildGroupItemBase
  implements FlatMapFunction<E, EdgeGroupItem> {

  /**
   * Avoid object initialization in each call.
   */
  private final EdgeGroupItem reuseEdgeGroupItem;

  /**
   * Creates map function.
   *
   * @param useLabel true, if vertex label shall be used
   * @param edgeLabelGroups stores grouping properties for edge labels
   */
  public BuildEdgeGroupItem(boolean useLabel, List<LabelGroup> edgeLabelGroups) {
    super(useLabel, edgeLabelGroups);
    this.reuseEdgeGroupItem = new EdgeGroupItem();
  }

  @Override
  public void flatMap(E edge, Collector<EdgeGroupItem> collector) throws Exception {
    boolean usedEdgeLabelGroup = false;
    reuseEdgeGroupItem.setSourceId(edge.getSourceId());
    reuseEdgeGroupItem.setTargetId(edge.getTargetId());

    // check if edge shall be grouped by a special set of keys
    for (LabelGroup edgeLabelGroup : getLabelGroups()) {
      if (edgeLabelGroup.getGroupingLabel().equals(edge.getLabel())) {
        usedEdgeLabelGroup = true;
        setGroupItem(reuseEdgeGroupItem, edge, edgeLabelGroup);
        collector.collect(reuseEdgeGroupItem);
      }
    }
    // standard grouping case
    if (!usedEdgeLabelGroup) {
      setGroupItem(reuseEdgeGroupItem, edge, getDefaultLabelGroup());
      collector.collect(reuseEdgeGroupItem);
    }
  }
}
