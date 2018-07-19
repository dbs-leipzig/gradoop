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
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Adds new graph id's to the edge if source and target vertex are part of
 * the same graph. Filters all edges between graphs.
 *
 * @param <E> EPGM edge Type
 */
@FunctionAnnotation.ForwardedFields("f0.id->id;f0.sourceId->sourceId;" +
  "f0.targetId->targetId;f0.label->label;f0.properties->properties")
public class AddNewGraphsToEdge<E extends Edge>
  implements FlatMapFunction<Tuple3<E, GradoopIdSet, GradoopIdSet>, E> {

  @Override
  public void flatMap(
    Tuple3<E, GradoopIdSet, GradoopIdSet> triple,
    Collector<E> collector) {
    GradoopIdSet sourceGraphs = triple.f1;
    GradoopIdSet targetGraphs = triple.f2;
    GradoopIdSet graphsToBeAdded = new GradoopIdSet();

    boolean filter = false;
    for (GradoopId id : sourceGraphs) {
      if (targetGraphs.contains(id)) {
        graphsToBeAdded.add(id);
        filter = true;
      }
    }

    if (filter) {
      E edge = triple.f0;
      edge.getGraphIds().addAll(graphsToBeAdded);
      collector.collect(edge);
    }
  }
}
