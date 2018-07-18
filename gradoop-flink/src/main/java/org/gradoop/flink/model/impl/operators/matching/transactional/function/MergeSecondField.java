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
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

import java.util.Iterator;

/**
 * Merges the second field of Tuple2, containing GradoopIds, to a GradoopIdSet.
 * @param <T> any type
 */
@FunctionAnnotation.ForwardedFields("f0")
@FunctionAnnotation.ReadFields("f1")
public class MergeSecondField<T>
  implements GroupReduceFunction<Tuple2<T, GradoopId>, Tuple2<T, GradoopIdSet>> {

  @Override
  public void reduce(Iterable<Tuple2<T, GradoopId>> iterable,
    Collector<Tuple2<T, GradoopIdSet>> collector) throws Exception {
    Iterator<Tuple2<T, GradoopId>> it = iterable.iterator();
    Tuple2<T, GradoopId> firstTuple = it.next();
    T firstField = firstTuple.f0;
    GradoopIdSet secondField = GradoopIdSet.fromExisting(firstTuple.f1);
    while (it.hasNext()) {
      GradoopId id = it.next().f1;
      secondField.add(id);
    }
    collector.collect(new Tuple2<>(firstField, secondField));
  }
}
