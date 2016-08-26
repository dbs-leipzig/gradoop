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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;

import java.util.List;

/**
 * Combines a group of {@link VertexGroupItem} instances.
 *
 * Note that this needs to be a subclass of {@link ReduceVertexGroupItems}. If
 * the base class would implement {@link GroupCombineFunction} and
 * {@link GroupReduceFunction}, a groupReduce call would automatically call
 * the combiner (which is not wanted).
 */
@FunctionAnnotation.ForwardedFields(
    "f0;" + // vertexId
    "f3;" + // label
    "f4"    // properties
)
public class CombineVertexGroupItems
  extends ReduceVertexGroupItems
  implements GroupCombineFunction<VertexGroupItem, VertexGroupItem> {

  /**
   * Creates group reduce function.
   *
   * @param useLabel          true, iff labels are used for grouping
   * @param vertexAggregators aggregate functions for super vertices
   */
  public CombineVertexGroupItems(boolean useLabel,
    List<PropertyValueAggregator> vertexAggregators) {
    super(useLabel, vertexAggregators);
  }

  @Override
  public void combine(Iterable<VertexGroupItem> values,
    Collector<VertexGroupItem> out) throws Exception {
    reduce(values, out);
  }
}
