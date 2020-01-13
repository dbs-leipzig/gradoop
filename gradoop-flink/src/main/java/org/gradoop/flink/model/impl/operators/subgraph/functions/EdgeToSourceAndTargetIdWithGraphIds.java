/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Flat map function mapping edges to tuples of (sourceId, graphIds) and (targetId, graphIds).
 *
 * @param <E> edge type
 */
public class EdgeToSourceAndTargetIdWithGraphIds<E extends Edge>
  implements FlatMapFunction<E, Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public void flatMap(E edge, Collector<Tuple2<GradoopId, GradoopIdSet>> collector)
    throws Exception {
    collector.collect(new Tuple2<>(edge.getSourceId(), edge.getGraphIds()));
    collector.collect(new Tuple2<>(edge.getTargetId(), edge.getGraphIds()));
  }
}
