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
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * (a,b),(a,c) => (a,{b,c})
 * (a,{b,c}),(a,{d,e}) => (a,{b,c,d,e})
 */
public class CollectGradoopIds implements
  GroupCombineFunction
    <Tuple2<GradoopId, GradoopIdSet>, Tuple2<GradoopId, GradoopIdSet>>,
  GroupReduceFunction
    <Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> mappings,
    Collector<Tuple2<GradoopId, GradoopIdSet>> collector) throws Exception {

    boolean first = true;
    GradoopId vertexId = null;
    GradoopIdSet btgIds = new GradoopIdSet();

    for (Tuple2<GradoopId, GradoopId> pair : mappings) {
      if (first) {
        vertexId = pair.f0;
        first = false;
      }
      btgIds.add(pair.f1);
    }
    collector.collect(new Tuple2<>(vertexId, btgIds));
  }

  @Override
  public void combine(Iterable<Tuple2<GradoopId, GradoopIdSet>> mappings,
    Collector<Tuple2<GradoopId, GradoopIdSet>> collector) throws Exception {

    boolean first = true;
    GradoopId vertexId = null;
    GradoopIdSet btgIds = null;

    for (Tuple2<GradoopId, GradoopIdSet> pair : mappings) {
      if (first) {
        vertexId = pair.f0;
        btgIds = pair.f1;
        first = false;
      }
      btgIds.addAll(pair.f1);
    }
    collector.collect(new Tuple2<>(vertexId, btgIds));
  }
}
