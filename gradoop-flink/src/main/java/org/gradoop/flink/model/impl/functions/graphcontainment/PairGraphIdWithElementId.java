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
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * graph-element -> (graphId, id)*
 *
 * @param <GE> EPGM graph element type
 */
@FunctionAnnotation.ForwardedFields("id->f1")
public class PairGraphIdWithElementId<GE extends GraphElement>
  implements FlatMapFunction<GE, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations.
   */
  private final Tuple2<GradoopId, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public void flatMap(GE ge, Collector<Tuple2<GradoopId, GradoopId>> collector)
      throws Exception {
    if (ge.getGraphCount() > 0) {
      reuseTuple.f1 = ge.getId();
      for (GradoopId graphId : ge.getGraphIds()) {
        reuseTuple.f0 = graphId;
        collector.collect(reuseTuple);
      }
    }
  }
}
