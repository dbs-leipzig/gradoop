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
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.tuple.ToIdWithLabel;
import org.gradoop.flink.model.impl.functions.tuple.Tuple2ToWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.operators.statistics.functions.BothLabels;
import org.gradoop.flink.model.impl.operators.statistics.functions.ToTargetIdWithLabel;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Computes the distribution of target and edge labels, e.g. the exact amount of (:A)<-[:a]-(),
 * for each existing target/edge label combination.
 */
public class TargetLabelAndEdgeLabelDistribution
  implements UnaryGraphToValueOperator<DataSet<WithCount<Tuple2<String, String>>>> {

  @Override
  public DataSet<WithCount<Tuple2<String, String>>> execute(LogicalGraph graph) {
    return Count.groupBy(graph.getVertices()
      .map(new ToIdWithLabel<>())
      .join(graph.getEdges().map(new ToTargetIdWithLabel<>()))
      .where(0).equalTo(0)
      .with(new BothLabels()))
      .map(new Tuple2ToWithCount<>());
  }
}
