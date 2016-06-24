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

package org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.id.GradoopId;

/**
 * (graphId, sourceId, targetId, edgeLabel, sourceLabel, targetLabel)
 */
public class EdgeTripleWithoutGraphId
  extends Tuple5<GradoopId, GradoopId, Integer, Integer, Integer> implements
  EdgeTriple {

  /**
   * Default constructor.
   */
  public EdgeTripleWithoutGraphId() {
  }

  /**
   * Valued Constructor.
   *
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param edgeLabel edge label
   * @param sourceLabel source vertex label
   * @param targetLabel target vertex label
   */
  public EdgeTripleWithoutGraphId(GradoopId sourceId, GradoopId targetId,
    Integer edgeLabel, Integer sourceLabel, Integer targetLabel) {
    super(sourceId, targetId, edgeLabel, sourceLabel, targetLabel);

  }

  public GradoopId getSourceId() {
    return this.f0;
  }

  public GradoopId getTargetId() {
    return this.f1;
  }

  public Integer getEdgeLabel() {
    return f2;
  }

  public Integer getSourceLabel() {
    return f3;
  }

  public Integer getTargetLabel() {
    return f4;
  }

}
