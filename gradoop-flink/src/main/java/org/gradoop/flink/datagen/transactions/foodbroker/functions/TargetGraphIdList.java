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
package org.gradoop.flink.datagen.transactions.foodbroker.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

import java.util.Iterator;

/**
 * Reduces for each target id of an edge all the graph ids to one graph id list.
 */
public class TargetGraphIdList
  implements GroupReduceFunction<Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> values,
    Collector<Tuple2<GradoopId, GradoopIdSet>> out) throws Exception {

    Iterator<Tuple2<GradoopId, GradoopId>> iterator = values.iterator();

    Tuple2<GradoopId, GradoopId> pair = iterator.next();

    // the target id is the same for each iterator element
    GradoopId targetId = pair.f0;
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(pair.f1);

    while (iterator.hasNext()) {
      graphIds.add(iterator.next().f1);
    }

    out.collect(new Tuple2<>(targetId, graphIds));
  }
}
