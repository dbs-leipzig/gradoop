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

package org.gradoop.flink.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples.FatVertex;


import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples.IdPair;

/**
 * Extracts all edges contained ina a {@link FatVertex}.
 *
 * fatVertex -> [(edgeId,sourceId,targetId)]*
 */
@FunctionAnnotation.ReadFields("f4")
public class EdgeTriple implements
  FlatMapFunction<FatVertex, Tuple3<GradoopId, GradoopId, GradoopId>> {

  /**
   * Reduce instantiations
   */
  private final Tuple3<GradoopId, GradoopId, GradoopId> reuseTuple;

  /**
   * Constructor
   */
  public EdgeTriple() {
    reuseTuple = new Tuple3<>();
  }

  @Override
  public void flatMap(FatVertex fatVertex,
    Collector<Tuple3<GradoopId, GradoopId, GradoopId>> collector) throws
    Exception {
    reuseTuple.f1 = fatVertex.getVertexId();
    for (IdPair idPair : fatVertex.getEdgeCandidates().keySet()) {
      reuseTuple.f0 = idPair.f0;
      reuseTuple.f2 = idPair.f1;
      collector.collect(reuseTuple);
    }
  }
}
