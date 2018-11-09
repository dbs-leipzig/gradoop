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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Maps an EPGM edge to a {@code Tuple2<GradoopId, GradoopId>} containing the source- and target-id.
 */
public class EdgeToSourceTargetTupleMap implements MapFunction<Edge, Tuple2<GradoopId, GradoopId>> {

  @Override
  public Tuple2<GradoopId, GradoopId> map(Edge edge) throws Exception {
    return Tuple2.of(edge.getSourceId(), edge.getTargetId());
  }
}
