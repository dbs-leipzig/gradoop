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
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples.TripleWithDirection;

/**
 * Clones and reverses the incoming triple:
 *
 * (e,s,t,c) -> (e,s,t,true,c),(e,t,s,false,c)
 *
 * Forwarded fields:
 *
 * f0->f0: edge id
 * f3->f4: query candidates
 */
@FunctionAnnotation.ForwardedFields("f0;f3->f4")
public class CloneAndReverse implements
  FlatMapFunction<TripleWithCandidates, TripleWithDirection> {

  /**
   * Reduce instantiations
   */
  private final TripleWithDirection reuseTuple;

  /**
   * Constructor
   */
  public CloneAndReverse() {
    reuseTuple = new TripleWithDirection();
  }

  @Override
  public void flatMap(TripleWithCandidates tripleWithCandidates,
    Collector<TripleWithDirection> collector) throws Exception {
    reuseTuple.setEdgeId(tripleWithCandidates.getEdgeId());
    reuseTuple.setCandidates(tripleWithCandidates.getCandidates());

    reuseTuple.setSourceId(tripleWithCandidates.getSourceId());
    reuseTuple.setTargetId(tripleWithCandidates.getTargetId());
    reuseTuple.setOutgoing(true);
    collector.collect(reuseTuple);

    reuseTuple.setSourceId(tripleWithCandidates.getTargetId());
    reuseTuple.setTargetId(tripleWithCandidates.getSourceId());
    reuseTuple.setOutgoing(false);
    collector.collect(reuseTuple);
  }
}
