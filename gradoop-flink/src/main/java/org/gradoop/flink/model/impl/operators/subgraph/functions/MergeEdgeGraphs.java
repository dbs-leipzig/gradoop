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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Reduces groups of tuples 4 consisting of 4 gradoop ids
 * into one tuple per group, containing the first three gradoop ids of the
 * first element in this group and a gradoop id set, containing all gradoop
 * ids in the fourth slot.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0;f2->f2;f3->f3")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f1")
public class MergeEdgeGraphs implements
  GroupReduceFunction<
    Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>,
    Tuple4<GradoopId, GradoopId, GradoopId, GradoopIdSet>> {

  @Override
  public void reduce(
    Iterable<Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>> iterable,
    Collector<
      Tuple4<GradoopId, GradoopId, GradoopId, GradoopIdSet>> collector) {

    GradoopIdSet set = new GradoopIdSet();

    boolean empty = true;
    GradoopId f0 = null;
    GradoopId f1 = null;
    GradoopId f2 = null;

    for (Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> tuple : iterable) {
      set.add(tuple.f3);
      empty = false;
      f0 = tuple.f0;
      f1 = tuple.f1;
      f2 = tuple.f2;
    }

    if (!empty) {
      collector.collect(new Tuple4<>(f0, f1, f2, set));
    }
  }
}
