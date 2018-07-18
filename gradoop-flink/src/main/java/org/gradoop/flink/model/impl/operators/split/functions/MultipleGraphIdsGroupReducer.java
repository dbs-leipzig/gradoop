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
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Reduce each group of vertices into a single vertex, whose graphId set
 * contains all graphs of each origin vertex.
 */
@FunctionAnnotation.ForwardedFields("f0")
public class MultipleGraphIdsGroupReducer
  implements GroupReduceFunction<Tuple2<GradoopId, GradoopId>,
  Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public void reduce(
    Iterable<Tuple2<GradoopId, GradoopId>> iterable,
    Collector<Tuple2<GradoopId, GradoopIdSet>> collector) {

    boolean first = true;
    GradoopId vertexId = null;
    GradoopIdSet idSet = new GradoopIdSet();

    for (Tuple2<GradoopId, GradoopId> vertexGraphPair : iterable) {
      if (first) {
        vertexId = vertexGraphPair.f0;
        first = false;
      }
      idSet.add(vertexGraphPair.f1);
    }
    collector.collect(new Tuple2<>(vertexId, idSet));
  }
}
