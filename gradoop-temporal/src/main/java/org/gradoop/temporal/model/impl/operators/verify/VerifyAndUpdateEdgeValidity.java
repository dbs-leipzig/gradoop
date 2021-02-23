/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.verify;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.verify.functions.UpdateEdgeValidity;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Verifies the edge set of a graph. This removes dangling edges (in the same way as
 * {@link org.gradoop.flink.model.impl.operators.verify.Verify} would) and updates the remaining edges so that
 * the edge is only valid when both its source- and target-vertex are valid.
 */
public class VerifyAndUpdateEdgeValidity implements UnaryBaseGraphToBaseGraphOperator<TemporalGraph> {

  @Override
  public TemporalGraph execute(TemporalGraph graph) {
    final DataSet<TemporalVertex> vertices = graph.getVertices();
    final DataSet<TemporalEdge> verifiedEdges = graph.getEdges()
      .join(vertices)
      .where(new SourceId<>())
      .equalTo(new Id<>())
      .with(new UpdateEdgeValidity())
      .name("Verify and update edges (1/2)")
      .join(vertices)
      .where(new TargetId<>())
      .equalTo(new Id<>())
      .with(new UpdateEdgeValidity())
      .name("Verify and update edges (2/2)");
    return graph.getFactory().fromDataSets(graph.getGraphHead(), vertices, verifiedEdges);
  }
}
