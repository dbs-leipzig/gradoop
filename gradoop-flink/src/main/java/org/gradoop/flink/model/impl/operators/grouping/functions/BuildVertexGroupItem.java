/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;

import java.util.List;

/**
 * Creates a minimal representation of vertex data to be used for label specific grouping.
 *
 * The output of that mapper is {@link VertexGroupItem} that contains
 * the vertex id, vertex label, vertex group properties and vertex aggregate
 * properties.
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("label;properties")
public class BuildVertexGroupItem
  extends BuildGroupItemBase
  implements FlatMapFunction<Vertex, VertexGroupItem> {

  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * Creates map function
   *
   * @param useLabel          true, if label shall be considered
   * @param vertexLabelGroups stores grouping properties for vertex labels
   */
  public BuildVertexGroupItem(boolean useLabel, List<LabelGroup> vertexLabelGroups) {
    super(useLabel, vertexLabelGroups);

    this.reuseVertexGroupItem = new VertexGroupItem();
    this.reuseVertexGroupItem.setSuperVertexId(GradoopId.NULL_VALUE);
    this.reuseVertexGroupItem.setSuperVertex(false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(Vertex vertex, Collector<VertexGroupItem> collector) throws Exception {
    boolean usedVertexLabelGroup = false;

    reuseVertexGroupItem.setVertexId(vertex.getId());

    // check if vertex shall be grouped by a special set of keys
    for (LabelGroup vertexLabelGroup : getLabelGroups()) {
      if (vertexLabelGroup.getGroupingLabel().equals(vertex.getLabel())) {
        usedVertexLabelGroup = true;
        setGroupItem(reuseVertexGroupItem, vertex, vertexLabelGroup);
        collector.collect(reuseVertexGroupItem);
      }
    }
    // standard grouping case
    if (!usedVertexLabelGroup) {
      setGroupItem(reuseVertexGroupItem, vertex, getDefaultLabelGroup());
      collector.collect(reuseVertexGroupItem);
    }
  }
}
