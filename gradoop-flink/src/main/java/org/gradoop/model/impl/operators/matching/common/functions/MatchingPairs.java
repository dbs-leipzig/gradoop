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

package org.gradoop.model.impl.operators.matching.common.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithSourceEdgeCandidates;

import java.util.List;

/**
 * Filters vertex-edge pairs based on their corresponding candidates.
 *
 * Forwarded Fields Second:
 *
 * f0:      edge id
 * f1:      source vertex id
 * f2->f3:  target vertex id
 */
@FunctionAnnotation.ForwardedFieldsSecond("f0;f1;f2->f3")
public class MatchingPairs extends RichFlatJoinFunction
  <IdWithCandidates, TripleWithCandidates, TripleWithSourceEdgeCandidates> {

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
  private final TripleWithSourceEdgeCandidates reuseTuple;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public MatchingPairs(final String query) {
    this.query = query;
    this.reuseTuple = new TripleWithSourceEdgeCandidates();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = new QueryHandler(query);
  }

  @Override
  public void join(IdWithCandidates sourceVertex, TripleWithCandidates edge,
    Collector<TripleWithSourceEdgeCandidates> collector) throws Exception {

    List<Long> newSourceCandidates = Lists.newArrayListWithCapacity(
      sourceVertex.getCandidates().size());
    List<Long> newEdgeCandidates = Lists.newArrayListWithCapacity(
      edge.getCandidates().size());

    for (Long eQ : edge.getCandidates()) {
      Long vQ = queryHandler.getEdgeById(eQ).getSourceVertexId();
      if (sourceVertex.getCandidates().contains(vQ)) {
        newSourceCandidates.add(vQ);
        newEdgeCandidates.add(eQ);
      }
    }

    if (!newEdgeCandidates.isEmpty()) {
      reuseTuple.setEdgeId(edge.getEdgeId());
      reuseTuple.setSourceId(edge.getSourceId());
      reuseTuple.setSourceCandidates(newSourceCandidates);
      reuseTuple.setTargetId(edge.getTargetId());
      reuseTuple.setEdgeCandidates(newEdgeCandidates);
      collector.collect(reuseTuple);
    }
  }
}
