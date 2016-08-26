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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.tuples.IdWithIdSet;

import java.util.List;
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
    "f3->f0.f3;" +  // label
    "f4->f0.f4"     // properties
)
public class TransposeVertexGroupItems
  extends ReduceVertexGroupItemBase
  implements GroupReduceFunction
  <VertexGroupItem, Tuple2<VertexGroupItem, IdWithIdSet>> {
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
   * @param useLabel          true, iff labels are used for grouping
   * @param vertexAggregators aggregate functions for super vertices
   */
  public TransposeVertexGroupItems(boolean useLabel,
    List<PropertyValueAggregator> vertexAggregators) {
    super(null, useLabel, vertexAggregators);
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

    boolean isFirst = true;

    Set<GradoopId> superVertexIds = Sets.newHashSet();

    for (VertexGroupItem groupItem : vertexGroupItems) {
      if (isFirst) {
        superVertexId = GradoopId.get();
        groupLabel            = groupItem.getGroupLabel();
        groupPropertyValues   = groupItem.getGroupingValues();

        isFirst = false;
      }
      // store the super vertex id created in the previous combiner
      superVertexIds.add(groupItem.getSuperVertexId());

      if (doAggregate()) {
        aggregate(groupItem.getAggregateValues());
      }
    }

    reuseInnerTuple.setId(superVertexId);
    reuseInnerTuple.setIdSet(GradoopIdSet.fromExisting(superVertexIds));

    reuseOuterTuple.f0 = createSuperVertexTuple(superVertexId, groupLabel,
      groupPropertyValues);
    reuseOuterTuple.f0.setSuperVertexId(superVertexId);
    reuseOuterTuple.f1 = reuseInnerTuple;

    // collect single item representing the whole group
    out.collect(reuseOuterTuple);

    resetAggregators();
  }
}
