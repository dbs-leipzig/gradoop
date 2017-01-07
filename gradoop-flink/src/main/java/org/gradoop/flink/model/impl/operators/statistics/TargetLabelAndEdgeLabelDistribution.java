/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
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
