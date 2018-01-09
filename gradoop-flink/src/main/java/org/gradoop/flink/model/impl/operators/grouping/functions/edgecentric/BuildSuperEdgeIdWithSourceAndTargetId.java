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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric
  .SuperEdgeIdWithSourceAndTargetId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;

/**
 * Assigns the source and target ids for each super edge id.
 */
public class BuildSuperEdgeIdWithSourceAndTargetId implements
  GroupReduceFunction<SuperVertexGroupItem, SuperEdgeIdWithSourceAndTargetId> {

  /**
   * Avoid object instantiation.
   */
  private SuperEdgeIdWithSourceAndTargetId reuseSuperEdgeIdWithSourceAndTargetId =
    new SuperEdgeIdWithSourceAndTargetId();

  @Override
  public void reduce(Iterable<SuperVertexGroupItem> iterable,
    Collector<SuperEdgeIdWithSourceAndTargetId> collector) throws Exception {
    boolean first = true;
    for (SuperVertexGroupItem superVertexGroupItem : iterable) {
      if (first) {
        reuseSuperEdgeIdWithSourceAndTargetId.setSuperEdgeId(superVertexGroupItem.getSuperEdgeId());
        reuseSuperEdgeIdWithSourceAndTargetId.setFirstSuperVertexId(
          superVertexGroupItem.getSuperVertexId());
        reuseSuperEdgeIdWithSourceAndTargetId.setFirstSuperVertexIds(
          superVertexGroupItem.getVertexIds());
        first = false;
      } else {
        reuseSuperEdgeIdWithSourceAndTargetId.setSecondSuperVertexId(
          superVertexGroupItem.getSuperVertexId());
        reuseSuperEdgeIdWithSourceAndTargetId.setSecondSuperVertexIds(
          superVertexGroupItem.getVertexIds());
      }
    }

    collector.collect(reuseSuperEdgeIdWithSourceAndTargetId);
  }
}
