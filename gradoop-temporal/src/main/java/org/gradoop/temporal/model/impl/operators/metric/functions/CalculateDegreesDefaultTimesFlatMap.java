/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.TreeMap;

/**
 * A flat map function calculating the resulting degree values for each vertex id by its corresponding degree
 * tree instance. This flat map uses default time values start an ending bounds.
 */
public class CalculateDegreesDefaultTimesFlatMap extends BaseCalculateDegrees
  implements FlatMapFunction<Tuple2<GradoopId, TreeMap<Long, Integer>>,
  Tuple4<GradoopId, Long, Long, Integer>> {

  @Override
  public void flatMap(Tuple2<GradoopId, TreeMap<Long, Integer>> vertexData,
    Collector<Tuple4<GradoopId, Long, Long, Integer>> collector) throws Exception {

    calculateDegreeAndCollect(vertexData.f0, vertexData.f1, TemporalElement.DEFAULT_TIME_FROM,
      TemporalElement.DEFAULT_TIME_TO, collector);
  }
}
