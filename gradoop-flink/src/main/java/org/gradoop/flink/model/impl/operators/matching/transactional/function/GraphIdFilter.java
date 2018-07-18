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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Filters a set of Tuple2 with GradoopIds in the first field by the
 * containment of this id in a broadcast set.
 *
 * @param <T> any type
 */
public class GraphIdFilter<T> extends RichFilterFunction<Tuple2<GradoopId, T>> {

  /**
   * Broadcast set of gradoop ids
   */
  private GradoopIdSet graphIds;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.graphIds = GradoopIdSet.fromExisting(
        getRuntimeContext().getBroadcastVariable("graph-ids"));
  }

  @Override
  public boolean filter(Tuple2<GradoopId, T> tuple2) throws Exception {
    return graphIds.contains(tuple2.f0);
  }
}
