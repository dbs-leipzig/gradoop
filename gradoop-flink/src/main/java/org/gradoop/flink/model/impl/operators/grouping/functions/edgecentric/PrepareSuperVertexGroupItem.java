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
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildGroupItemBase;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;

import java.util.List;

/**
 * Returns tuples which assigns each source/target set of gradoop ids the edge id.
 */
@FunctionAnnotation.ForwardedFields("f0->f2")
@FunctionAnnotation.ReadFields("f2;f3")
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
