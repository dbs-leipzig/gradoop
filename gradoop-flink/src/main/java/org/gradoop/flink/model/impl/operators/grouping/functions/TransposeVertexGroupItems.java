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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.tuples.IdWithIdSet;

import java.util.Set;

/**
 * Takes a group of {@link VertexGroupItem} as input where each item represents
 * the same group but stores a unique super vertex ids. This function creates
 * a single {@link VertexGroupItem} that represents the whole group and also
 * outputs a set containing all the super vertex ids of the incoming items.
 *
 * Example input:
 * (1,A)
 * (2,A)
 * (3,B)
 * (4,B)
 *
 * Example output:
 * ((10,A),(10,[1,2]))
 * ((20,B),(20,[3,4]))
 */
@FunctionAnnotation.ForwardedFields(
    "f0->f0.f0;" +  // vertexId
    "f2->f0.f2;" +  // label
    "f3->f0.f3;" +  // properties
    "f6->f0.f6"     // label group
)
@FunctionAnnotation.ReadFields("f4")
public class TransposeVertexGroupItems
  extends ReduceVertexGroupItemBase
  implements GroupReduceFunction<VertexGroupItem, Tuple2<VertexGroupItem, IdWithIdSet>> {
  /**
   * Reduce object instantiation
   */
  private final Tuple2<VertexGroupItem, IdWithIdSet> reuseOuterTuple;
  /**
   * Reduce object instantiation
   */
  private final IdWithIdSet reuseInnerTuple;
  /**
   * Creates group reduce function.
   *
   * @param useLabel                        true, iff labels are used for grouping
   */
  public TransposeVertexGroupItems(boolean useLabel) {
    super(useLabel);
    this.reuseOuterTuple = new Tuple2<>();
    this.reuseInnerTuple = new IdWithIdSet();
  }

  @Override
  public void reduce(Iterable<VertexGroupItem> vertexGroupItems,
    Collector<Tuple2<VertexGroupItem, IdWithIdSet>> out)
      throws Exception {

    GradoopId superVertexId               = null;
    String groupLabel                     = null;
    PropertyValueList groupPropertyValues = null;
    LabelGroup vertexLabelGroup           = null;

    boolean isFirst = true;

    Set<GradoopId> superVertexIds = Sets.newHashSet();

    for (VertexGroupItem groupItem : vertexGroupItems) {
      if (isFirst) {
        superVertexId = GradoopId.get();
        groupLabel            = groupItem.getGroupLabel();
        groupPropertyValues   = groupItem.getGroupingValues();
        vertexLabelGroup      = groupItem.getLabelGroup();

        isFirst = false;
      }
      // store the super vertex id created in the previous combiner
      superVertexIds.add(groupItem.getSuperVertexId());

      if (doAggregate(groupItem.getLabelGroup().getAggregators())) {
        aggregate(groupItem.getAggregateValues(), vertexLabelGroup.getAggregators());
      }
    }

    reuseInnerTuple.setId(superVertexId);
    reuseInnerTuple.setIdSet(GradoopIdSet.fromExisting(superVertexIds));

    reuseOuterTuple.f0 = createSuperVertexTuple(superVertexId, groupLabel,
      groupPropertyValues, vertexLabelGroup.getAggregators());
    reuseOuterTuple.f0.setSuperVertexId(superVertexId);
    reuseOuterTuple.f0.setLabelGroup(vertexLabelGroup);
    reuseOuterTuple.f1 = reuseInnerTuple;

    // collect single item representing the whole group
    resetAggregators(reuseOuterTuple.f0.getLabelGroup().getAggregators());
    out.collect(reuseOuterTuple);
  }
}
