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

package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EdgeStep;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching;

/**
 * Converts an edge into a step edge according to the traversal.
 *
 * Note that forwarded fields annotations need to be declared at the calling
 * site for this function.
 *
 * @param <K> key type
 */
public class BuildEdgeStep<K> extends RichMapFunction<TripleWithCandidates<K>, EdgeStep<K>> {

  /**
   * Reduce instantiations
   */
  private final EdgeStep<K> reuseTuple;

  /**
   * Traversal code to determine correct step.
   */
  private final TraversalCode traversalCode;

  /**
   * True, if edge is outgoing
   */
  private boolean isOutgoing;

  /**
   * Constructor
   *
   * @param traversalCode traversal code
   */
  public BuildEdgeStep(TraversalCode traversalCode) {
    this.traversalCode = traversalCode;
    reuseTuple = new EdgeStep<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    int step = (int) getRuntimeContext()
      .getBroadcastVariable(ExplorativePatternMatching.BC_SUPERSTEP).get(0);
    isOutgoing = traversalCode.getStep(step - 1).isOutgoing();
  }

  @Override
  public EdgeStep<K> map(TripleWithCandidates<K> t) throws Exception {
    reuseTuple.setEdgeId(t.getEdgeId());
    reuseTuple.setTiePointId(isOutgoing ? t.getSourceId() : t.getTargetId());
    reuseTuple.setNextId(isOutgoing ? t.getTargetId() : t.getSourceId());
    return reuseTuple;
  }
}
