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
package org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Set;

/**
 * Stores information about the super edge id together with the source/target ids and the set of
 * base vertex ids.
 */
public class SuperEdgeIdWithSourceAndTargetId
  extends Tuple5<GradoopId, GradoopId, Set<GradoopId>, GradoopId, Set<GradoopId>> {

  public void setSuperEdgeId(GradoopId superEdgeId) {
    f0 = superEdgeId;
  }

  public GradoopId getFirstSuperVertexId() {
    return f1;
  }

  public void setFirstSuperVertexId(GradoopId firstSuperVertexId) {
    f1 = firstSuperVertexId;
  }

  public Set<GradoopId> getFirstSuperVertexIds() {
    return f2;
  }

  public void setFirstSuperVertexIds(Set<GradoopId> firstSuperVertexIds) {
    f2 = firstSuperVertexIds;
  }

  public GradoopId getSecondSuperVertexId() {
    return f3;
  }

  public void setSecondSuperVertexId(GradoopId secondSuperVertexId) {
    f3 = secondSuperVertexId;
  }

  public Set<GradoopId> getSecondSuperVertexIds() {
    return f4;
  }

  public void setSecondSuperVertexIds(Set<GradoopId> secondSuperVertexIds) {
    f4 = secondSuperVertexIds;
  }
}
