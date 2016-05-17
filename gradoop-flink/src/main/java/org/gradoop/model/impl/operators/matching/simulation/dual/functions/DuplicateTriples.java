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

package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithDirection;

/**
 * Duplicates and reverses the incoming triple.
 *
 * Forwarded fields:
 *
 * f0->f0: edge id
 * f3->f4: query candidates
 */
@FunctionAnnotation.ForwardedFields("f0;f3->f4")
public class DuplicateTriples implements
  FlatMapFunction<MatchingTriple, TripleWithDirection> {

  private final TripleWithDirection reuseTuple;

  public DuplicateTriples() {
    reuseTuple = new TripleWithDirection();
  }

  @Override
  public void flatMap(MatchingTriple matchingTriple,
    Collector<TripleWithDirection> collector) throws Exception {
    reuseTuple.setEdgeId(matchingTriple.getEdgeId());
    reuseTuple.setCandidates(matchingTriple.getQueryCandidates());

    reuseTuple.setSourceId(matchingTriple.getSourceVertexId());
    reuseTuple.setTargetId(matchingTriple.getTargetVertexId());
    reuseTuple.isOutgoing(true);
    collector.collect(reuseTuple);

    reuseTuple.setSourceId(matchingTriple.getTargetVertexId());
    reuseTuple.setTargetId(matchingTriple.getSourceVertexId());
    reuseTuple.isOutgoing(false);
    collector.collect(reuseTuple);
  }
}
