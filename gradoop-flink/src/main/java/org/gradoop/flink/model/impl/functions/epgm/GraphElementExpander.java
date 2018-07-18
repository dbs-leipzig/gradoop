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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.util.GradoopConstants;

/**
 * Takes an object of type GraphElement, and creates a tuple2 for each
 *  gradoop id containing in the set of the object and the object.
 * element => (graphId, element)
 * @param <EL> graph element type
 */
@FunctionAnnotation.ForwardedFields("*->f1")
public class GraphElementExpander<EL extends GraphElement>
  implements FlatMapFunction<EL, Tuple2<GradoopId, EL>> {

  /**
   * reuse tuple
   */
  private Tuple2<GradoopId, EL> reuse;

  /**
   * constructor
   */
  public GraphElementExpander() {
    reuse = new Tuple2<>();
  }

  @Override
  public void flatMap(EL element, Collector<Tuple2<GradoopId, EL>> collector) throws Exception {
    reuse.f1 = element;
    for (GradoopId graphId : element.getGraphIds()) {
      reuse.f0 = graphId;
      collector.collect(reuse);
    }
    // assign entities with no graph to the DB graph
    if (element.getGraphCount() == 0) {
      reuse.f0 = GradoopConstants.DB_GRAPH_ID;
      collector.collect(reuse);
    }
  }
}
