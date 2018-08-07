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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.statistics.SourceLabelAndEdgeLabelDistribution;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Computes {@link SourceLabelAndEdgeLabelDistribution} for a given logical graph and write it in a CSV file.
 */
public class SourceAndEdgeLabelDistributionPreparer  implements
UnaryGraphToValueOperator<MapOperator<WithCount<Tuple2<String, String>>,
Tuple3<String, String, Long>>> {

  /**
   * Prepares the statistic a source and edge label distribution.
   * @param graph the logical graph for the calculation.
   * @return tuples with the containing statistics.
   */
  @Override
  public MapOperator<WithCount<Tuple2<String, String>>, Tuple3<String, String, Long>>
  execute(final LogicalGraph graph) {
    return new SourceLabelAndEdgeLabelDistribution()
        .execute(graph)
        .map(value -> Tuple3.of(value.f0.f0, value.f0.f1, value.f1))
        .returns(new TypeHint<Tuple3<String, String, Long>>() { });
  }
}
