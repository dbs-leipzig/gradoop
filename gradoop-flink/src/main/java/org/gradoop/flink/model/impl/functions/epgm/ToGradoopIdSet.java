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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * id1,..,idn => {id1,..,idn}
 */
public class ToGradoopIdSet
  implements GroupReduceFunction<GradoopId, GradoopIdSet> {

  @Override
  public void reduce(Iterable<GradoopId> iterable,
    Collector<GradoopIdSet> collector) throws Exception {

    GradoopIdSet ids = new GradoopIdSet();

    for (GradoopId id : iterable) {
      ids.add(id);
    }

    collector.collect(ids);
  }
}
