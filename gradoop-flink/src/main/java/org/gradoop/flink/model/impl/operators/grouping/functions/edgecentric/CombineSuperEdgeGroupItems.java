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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;

/**
 * Creates a local {@link SuperEdgeGroupItem} which represents one group of super edges based on the
 * grouping settings.
 */
public class CombineSuperEdgeGroupItems
  extends ReduceSuperEdgeGroupItems
  implements GroupCombineFunction<SuperEdgeGroupItem, SuperEdgeGroupItem> {

  /**
   * Creates group combine function.
   *
   * @param useLabel true, iff labels are used for grouping
   * @param sourceSpecificGrouping true if the source vertex shall be considered for grouping
   * @param targetSpecificGrouping true if the target vertex shall be considered for grouping
   */
  public CombineSuperEdgeGroupItems(boolean useLabel, boolean sourceSpecificGrouping,
    boolean targetSpecificGrouping) {
    super(useLabel, sourceSpecificGrouping, targetSpecificGrouping);
  }

  @Override
  public void combine(Iterable<SuperEdgeGroupItem> values,
    Collector<SuperEdgeGroupItem> out) throws Exception {
    reduce(values, out);
  }
}
