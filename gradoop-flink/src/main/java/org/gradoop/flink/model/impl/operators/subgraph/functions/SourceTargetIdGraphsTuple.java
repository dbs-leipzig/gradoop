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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * For each edge, collect two tuple 2 containing its source or target id in the
 * first field and all the graphs this edge is contained in in its second field.
 *
 * @param <E> epgm edge type
 */

@FunctionAnnotation.ReadFields("sourceId;targetId")
@FunctionAnnotation.ForwardedFields("graphIds->f1")
public class SourceTargetIdGraphsTuple<E extends Edge>
  implements FlatMapFunction<E, Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public void flatMap(
    E e,
    Collector<Tuple2<GradoopId, GradoopIdSet>> collector) throws
    Exception {

    collector.collect(new Tuple2<>(e.getSourceId(), e.getGraphIds()));
    collector.collect(new Tuple2<>(e.getTargetId(), e.getGraphIds()));
  }
}
