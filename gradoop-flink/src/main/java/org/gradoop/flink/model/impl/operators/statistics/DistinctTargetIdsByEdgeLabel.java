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
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Tuple2ToWithCount;
import org.gradoop.flink.model.impl.operators.count.functions.Tuple2FromTupleWithObjectAnd1L;
import org.gradoop.flink.model.impl.operators.statistics.functions.ToTargetIdWithLabel;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Computes the number of distinct source ids per edge label.
 */
public class DistinctTargetIdsByEdgeLabel
  implements UnaryGraphToValueOperator<DataSet<WithCount<String>>> {

  @Override
  public DataSet<WithCount<String>> execute(LogicalGraph graph) {
    return graph.getEdges()
      .map(new ToTargetIdWithLabel<>())
      .groupBy(0, 1)
      .first(1)
      .<Tuple1<String>>project(1)
      .map(new Tuple2FromTupleWithObjectAnd1L<>())
      .groupBy(0)
      .sum(1)
      .map(new Tuple2ToWithCount<>());
  }
}
