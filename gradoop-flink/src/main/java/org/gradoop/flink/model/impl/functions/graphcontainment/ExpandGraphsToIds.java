/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Takes a graph element as input and collects all graph ids the element is
 * contained in.
 *
 * graph-element -> {graph id 1, graph id 2, ..., graph id n}
 *
 * @param <GE> EPGM graph element (i.e. vertex / edge)
 */
@FunctionAnnotation.ReadFields("graphIds")
public class ExpandGraphsToIds<GE extends GraphElement>
  implements FlatMapFunction<GE, GradoopId> {

  @Override
  public void flatMap(GE ge, Collector<GradoopId> collector) {
    for (GradoopId gradoopId : ge.getGraphIds()) {
      collector.collect(gradoopId);
    }
  }
}
