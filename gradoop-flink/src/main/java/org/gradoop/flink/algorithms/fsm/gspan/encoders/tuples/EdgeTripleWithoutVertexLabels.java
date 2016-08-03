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

package org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (graphId, sourceId, targetId, edgeLabel)
 */
public class EdgeTripleWithoutVertexLabels
  extends Tuple4<GradoopId, GradoopId, GradoopId, Integer> {

  /**
   * Default constructor.
   */
  public EdgeTripleWithoutVertexLabels() {
  }

  /**
   * Valued Constructor.
   *
   * @param graphId graph id
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param edgeLabel edge label
   */
  public EdgeTripleWithoutVertexLabels(GradoopId graphId,
    GradoopId sourceId, GradoopId targetId, Integer edgeLabel) {
    super(graphId, sourceId, targetId, edgeLabel);
  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  public GradoopId getSourceId() {
    return this.f1;
  }

  public GradoopId getTargetId() {
    return this.f2;
  }

  public Integer getEdgeLabel() {
    return f3;
  }
}
