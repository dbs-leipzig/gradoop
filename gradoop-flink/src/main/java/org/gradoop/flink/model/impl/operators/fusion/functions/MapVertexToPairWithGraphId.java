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
package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Demultiplexes a vertex by associating its graphId
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class MapVertexToPairWithGraphId
  implements FlatMapFunction<Vertex, Tuple2<Vertex, GradoopId>> {

  /**
   * Reusable element ot be returned
   */
  private final Tuple2<Vertex,   GradoopId> reusableTuple;

  /**
   * Default constructor
   */
  public MapVertexToPairWithGraphId() {
    reusableTuple = new Tuple2<>();
  }

  @Override
  public void flatMap(Vertex value, Collector<Tuple2<Vertex, GradoopId>> out)
      throws Exception {
    if (value != null) {
      for (GradoopId id : value.getGraphIds()) {
        reusableTuple.f0 = value;
        reusableTuple.f1 = id;
        out.collect(reusableTuple);
      }
    }
  }
}
