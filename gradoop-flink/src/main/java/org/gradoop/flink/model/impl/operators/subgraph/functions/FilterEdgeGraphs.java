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

package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Filter the edge tuples. Check if each new graph the edge is contained in
 * also contains the source and target of this edge. Only collect the edge if
 * it is valid in at least one graph.
 */

@FunctionAnnotation.ReadFields("f1;f2;f3")
@FunctionAnnotation.ForwardedFields("f0->f0")
public class FilterEdgeGraphs
  implements FlatMapFunction<
  Tuple4<GradoopId, GradoopIdSet, GradoopIdSet, GradoopIdSet>,
  Tuple2<GradoopId, GradoopIdSet>> {

  /**
   * Reduce object instantiations
   */
  private Tuple2<GradoopId, GradoopIdSet> reuseTuple = new Tuple2<>();

  @Override
  public void flatMap(
    Tuple4<GradoopId, GradoopIdSet, GradoopIdSet, GradoopIdSet> edgeTuple,
    Collector<Tuple2<GradoopId, GradoopIdSet>> collector) throws Exception {
    GradoopIdSet set = new GradoopIdSet();
    for (GradoopId edgeGraph : edgeTuple.f3) {
      for (GradoopId sourceGraph : edgeTuple.f1) {
        if (edgeGraph.equals(sourceGraph)) {
          for (GradoopId targetGraph : edgeTuple.f2) {
            if (edgeGraph.equals(targetGraph)) {
              set.add(edgeGraph);
            }
          }
        }
      }
    }
    if (!set.isEmpty()) {
      reuseTuple.f0 = edgeTuple.f0;
      reuseTuple.f1 = set;
      collector.collect(reuseTuple);
    }
  }
}
