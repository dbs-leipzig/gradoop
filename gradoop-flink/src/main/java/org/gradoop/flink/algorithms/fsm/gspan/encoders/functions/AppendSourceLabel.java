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

package org.gradoop.flink.algorithms.fsm.gspan.encoders.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples.EdgeTripleWithoutTargetLabel;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples.EdgeTripleWithoutVertexLabels;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples.VertexIdLabel;

/**
 * (graphId, sourceId, targetId, edgeLabel) |><| (sourceId, sourceLabel)
 * => (graphId, sourceId, targetId, edgeLabel, sourceLabel)
 */
public class AppendSourceLabel
  implements JoinFunction
  <EdgeTripleWithoutVertexLabels, VertexIdLabel, EdgeTripleWithoutTargetLabel> {

  @Override
  public EdgeTripleWithoutTargetLabel join(
    EdgeTripleWithoutVertexLabels edge, VertexIdLabel source) throws Exception {
    return new EdgeTripleWithoutTargetLabel(
      edge.getGraphId(),
      edge.getSourceId(),
      edge.getTargetId(),
      edge.getEdgeLabel(),
      source.getLabel()
    );
  }
}
