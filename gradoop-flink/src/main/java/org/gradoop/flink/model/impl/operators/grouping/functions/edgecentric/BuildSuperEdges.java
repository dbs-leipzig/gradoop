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

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildBase;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric
  .SuperEdgeIdWithSourceAndTargetId;

/**
 * Creates a new super edge representing an edge group. The edge stores the
 * group label, the group property value and the aggregate values for its group.
 */
public class BuildSuperEdges
  extends BuildBase
  implements JoinFunction<SuperEdgeGroupItem, SuperEdgeIdWithSourceAndTargetId, Edge> {

  /**
   * Edge edgeFactory.
   */
  private final EdgeFactory edgeFactory;

  /**
   * Creates map function.
   *
   * @param useLabel true, if vertex label shall be considered
   * @param edgeFactory edge factory
   */
  public BuildSuperEdges(boolean useLabel, EdgeFactory edgeFactory) {
    super(useLabel);
    this.edgeFactory = edgeFactory;
  }

  @Override
  public Edge join(SuperEdgeGroupItem superEdgeGroupItem,
    SuperEdgeIdWithSourceAndTargetId superEdgeIdWithSourceAndTargetId) throws Exception {

    GradoopId sourceId = null;
    GradoopId targetId = null;

    // assign the first id to either the source or the target
    if (superEdgeIdWithSourceAndTargetId.getFirstSuperVertexIds()
      .equals(superEdgeGroupItem.getSourceIds())) {
      sourceId = superEdgeIdWithSourceAndTargetId.getFirstSuperVertexId();
    } else if (superEdgeIdWithSourceAndTargetId.getFirstSuperVertexIds()
      .equals(superEdgeGroupItem.getTargetIds())) {
      targetId = superEdgeIdWithSourceAndTargetId.getFirstSuperVertexId();
    }
    // if source/target is not assigned yet then assign the second id
    if (sourceId == null && superEdgeIdWithSourceAndTargetId.getSecondSuperVertexIds()
      .equals(superEdgeGroupItem.getSourceIds())) {
      sourceId = superEdgeIdWithSourceAndTargetId.getSecondSuperVertexId();
    } else if (targetId == null && superEdgeIdWithSourceAndTargetId.getSecondSuperVertexIds()
      .equals(superEdgeGroupItem.getTargetIds())) {
      targetId = superEdgeIdWithSourceAndTargetId.getSecondSuperVertexId();
    }

    Edge superEdge = edgeFactory.initEdge(superEdgeGroupItem.getSuperEdgeId(), sourceId, targetId);

    superEdge.setLabel(superEdgeGroupItem.getGroupLabel());
    setGroupProperties(
      superEdge,
      superEdgeGroupItem.getGroupingValues(),
      superEdgeGroupItem.getLabelGroup());
    setAggregateValues(
      superEdge,
      superEdgeGroupItem.getAggregateValues(),
      superEdgeGroupItem.getLabelGroup().getAggregators());

    return superEdge;
  }
}
