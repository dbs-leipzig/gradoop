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
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Returns a map from each gradoop id to the object.
 *
 * @param <T> Type of the maps value
 */
public class MasterDataMapFromTuple<T>
  implements GroupReduceFunction<Tuple2<GradoopId, T>, Map<GradoopId, T>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, T>> iterable,
    Collector<Map<GradoopId, T>> collector) throws Exception {
    Map<GradoopId, T> map = Maps.newHashMap();
    // fill map with all tuple pairs
    for (Tuple2<GradoopId, T> tuple : iterable) {
      map.put(tuple.f0, tuple.f1);
    }
    collector.collect(map);
  }
}
