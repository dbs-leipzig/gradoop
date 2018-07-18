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
package org.gradoop.flink.model.impl.operators.distinction.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;

/**
 * Distinction function that just selects the first graph head of an isomorphic group.
 */
public class FirstGraphHead implements GraphHeadReduceFunction {

  @Override
  public void reduce(Iterable<Tuple2<String, GraphHead>> iterable,
    Collector<GraphHead> collector) throws Exception {
    collector.collect(iterable.iterator().next().f1);
  }
}
