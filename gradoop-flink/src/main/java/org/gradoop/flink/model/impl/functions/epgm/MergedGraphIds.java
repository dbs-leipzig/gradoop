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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopIdSet;

import java.util.Iterator;

/**
 * Creates a single graph element which is contained in all graphs that the
 * input elements are contained in.
 *
 * GraphElement* -> GraphElement
 *
 * @param <GE> EPGM graph element type
 */
public class MergedGraphIds<GE extends GraphElement>
  implements GroupCombineFunction<GE, GE>, GroupReduceFunction<GE, GE>,
  JoinFunction<GE, GE, GE> {


  @Override
  public void combine(Iterable<GE> values, Collector<GE> out) throws Exception {
    reduce(values, out);
  }

  @Override
  public void reduce(Iterable<GE> values, Collector<GE> out) throws Exception {
    Iterator<GE> iterator = values.iterator();
    GE result = iterator.next();
    GradoopIdSet graphIds = result.getGraphIds();
    while (iterator.hasNext()) {
      graphIds.addAll(iterator.next().getGraphIds());
    }
    out.collect(result);
  }

  @Override
  public GE join(GE first, GE second) throws Exception {
    if (first == null) {
      return second;
    }
    first.getGraphIds().addAll(second.getGraphIds());
    return first;
  }
}
