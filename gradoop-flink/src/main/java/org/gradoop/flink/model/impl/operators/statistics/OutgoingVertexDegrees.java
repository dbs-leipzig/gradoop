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
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.operators.statistics.functions.SetOrCreateWithCount;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Computes the outgoing degree for each vertex.
 */
public class OutgoingVertexDegrees
  implements UnaryGraphToValueOperator<DataSet<WithCount<GradoopId>>> {

  @Override
  public DataSet<WithCount<GradoopId>> execute(LogicalGraph graph) {
    return new EdgeValueDistribution<>(new SourceId<>()).execute(graph)
      .rightOuterJoin(graph.getVertices().map(new Id<>()))
      .where(0).equalTo("*")
      .with(new SetOrCreateWithCount());
  }
}
