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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Reduces groups of tuples 2 containin two gradoop ids into one tuple
 * per group, containing the first gradoop id and a gradoop id set,
 * containing all the second gradoop ids.
 */
@FunctionAnnotation.ReadFields("f1")
@FunctionAnnotation.ForwardedFields("f0->f0")
public class MergeTupleGraphs implements
  GroupReduceFunction<
    Tuple2<GradoopId, GradoopId>,
    Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> iterable,
    Collector<Tuple2<GradoopId, GradoopIdSet>> collector) throws Exception {
    GradoopIdSet set = new GradoopIdSet();
    boolean empty = true;
    GradoopId first = null;
    for (Tuple2<GradoopId, GradoopId> tuple : iterable) {
      set.add(tuple.f1);
      empty = false;
      first = tuple.f0;
    }
    if (!empty) {
      collector.collect(new Tuple2<>(first, set));
    }
  }
}
