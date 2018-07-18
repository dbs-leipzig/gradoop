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
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Maps an edge  tuples 4 containing the id of this edge, the id of its
 * source and target and a graph this edge is contained in. One tuple per graph.
 *
 * @param <E> epgm edge type
 */

@FunctionAnnotation.ReadFields("graphIds")
@FunctionAnnotation.ForwardedFields("id->f0;sourceId->f1;targetId->f2")
public class IdSourceTargetGraphTuple<E extends Edge>
  implements FlatMapFunction<
  E, Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>> {

  @Override
  public void flatMap(
    E edge,
    Collector<Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>> collector) {

    for (GradoopId graphId : edge.getGraphIds()) {
      collector.collect(new Tuple4<>(
        edge.getId(),
        edge.getSourceId(),
        edge.getTargetId(),
        graphId));
    }

  }
}
