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
package org.gradoop.flink.model.impl.operators.statistics.writer;

import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.operators.sampling.statistics.DegreeCentrality;

/**
 * Computes {@link DegreeCentrality} for a given logical graph.
 */
public class DegreeCentralityPreparer implements
  UnaryGraphToValueOperator<MapOperator<Double, Tuple1<Double>>> {

  /**
   * Prepares the statistic for the degree centrality and wraps it into a {@link Tuple1}.
   *
   * @param graph the logical graph for the calculation.
   * @return tuple with degree centrality value of graph
   */
  @Override
  public MapOperator<Double, Tuple1<Double>> execute(final LogicalGraph graph) {
    return new DegreeCentrality()
      .execute(graph)
      .map(new ObjectTo1<>());
  }
}
