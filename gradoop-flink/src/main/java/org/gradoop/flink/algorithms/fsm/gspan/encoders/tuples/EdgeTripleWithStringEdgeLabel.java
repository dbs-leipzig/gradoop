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

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * (graphId, sourceId, targetId, edgeLabel, sourceLabel, targetLabel)
 *
 * @param <IDT> Id type
 */
public class EdgeTripleWithStringEdgeLabel<IDT>
  extends Tuple5<IDT, IDT, String, Integer, Integer> {

  /**
   * Default constructor.
   */
  public EdgeTripleWithStringEdgeLabel() {
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
  public EdgeTripleWithStringEdgeLabel(IDT sourceId, IDT targetId,
    String edgeLabel, Integer sourceLabel, Integer targetLabel) {
    super(sourceId, targetId, edgeLabel, sourceLabel, targetLabel);

  }

  public IDT getSourceId() {
    return this.f0;
  }

  public IDT getTargetId() {
    return this.f1;
  }

  public String getEdgeLabel() {
    return f2;
  }

  public Integer getSourceLabel() {
    return f3;
  }

  public Integer getTargetLabel() {
    return f4;
  }

}
