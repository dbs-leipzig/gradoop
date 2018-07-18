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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;

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
    "f4;"  + // properties
    "f6"    // label group
)
public class CombineVertexGroupItems
  extends ReduceVertexGroupItems
  implements GroupCombineFunction<VertexGroupItem, VertexGroupItem> {

  /**
   * Creates group reduce function.
   *
   * @param useLabel                        true, iff labels are used for grouping
   */
  public CombineVertexGroupItems(boolean useLabel) {
    super(useLabel);
  }

  @Override
  public void combine(Iterable<VertexGroupItem> values,
    Collector<VertexGroupItem> out) throws Exception {
    reduce(values, out);
  }
}
