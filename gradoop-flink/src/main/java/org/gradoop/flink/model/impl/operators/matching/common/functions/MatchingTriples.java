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

package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.tuples
  .IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;

import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithSourceEdgeCandidates;

import org.s1ck.gdl.model.Edge;

/**
 * Takes a vertex-edge pair and the corresponding target vertex as input and
 * evaluates, if the triple matches against the query graph. The output is
 * a {@link TripleWithCandidates} containing all query candidates for the
 * triple.
 *
 * Forwarded fields first:
 *
 * f0:      edge id
 * f1:      source vertex id
 * f3->f2:  target vertex id
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0;f1;f3->f2")
public class MatchingTriples extends RichFlatJoinFunction
  <TripleWithSourceEdgeCandidates, IdWithCandidates, TripleWithCandidates> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * GDL query
   */
  private final String query;

  /**
   * Query handler
   */
  private transient QueryHandler queryHandler;

  /**
   * Reduce instantiations
   */
  private final TripleWithCandidates reuseTriple;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public MatchingTriples(final String query) {
    this.query = query;
    this.reuseTriple = new TripleWithCandidates();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = new QueryHandler(query);
  }

  @Override
  public void join(TripleWithSourceEdgeCandidates pair,
    IdWithCandidates target,
    Collector<TripleWithCandidates> collector) throws Exception {

    boolean[] edgeCandidates = pair.getEdgeCandidates();
    boolean[] newEdgeCandidates = new boolean[edgeCandidates.length];
    boolean edgeStillValid = false;

    for (int i = 0; i < edgeCandidates.length; i++) {
      if (edgeCandidates[i]) {
        Edge e = queryHandler.getEdgeById((long) i);
        if (pair.getSourceCandidates()[e.getSourceVertexId().intValue()] &&
          target.getCandidates()[e.getTargetVertexId().intValue()]) {
          newEdgeCandidates[i] = true;
          edgeStillValid = true;
        }
      }
    }

    if (edgeStillValid) {
      reuseTriple.setEdgeId(pair.getEdgeId());
      reuseTriple.setSourceId(pair.getSourceId());
      reuseTriple.setTargetId(target.getId());
      reuseTriple.setCandidates(newEdgeCandidates);
      collector.collect(reuseTriple);
    }
  }
}
