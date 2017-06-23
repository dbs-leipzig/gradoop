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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.EdgeWithTiePoint;

/**
 * Extract the join key from an edge embedding and stores both in an {@link EdgeWithTiePoint}
 */
public class ExtractKeyedCandidateEdges
  extends RichMapFunction<Embedding, EdgeWithTiePoint> {

  /**
   * Reuse Tuple
   */
  private EdgeWithTiePoint reuseEdgeWitTiePoint;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.reuseEdgeWitTiePoint = new EdgeWithTiePoint();
  }

  @Override
  public EdgeWithTiePoint map(Embedding edge) throws Exception {
    reuseEdgeWitTiePoint.setSource(edge.getId(0));
    reuseEdgeWitTiePoint.setId(edge.getId(1));
    if (edge.size() == 3) {
      // normal edge
      reuseEdgeWitTiePoint.setTarget(edge.getId(2));
    } else {
      // loop edge
      reuseEdgeWitTiePoint.setTarget(edge.getId(0));
    }

    return reuseEdgeWitTiePoint;
  }
}
