/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
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
public class MatchingTriples extends RichFlatJoinFunction<TripleWithSourceEdgeCandidates<GradoopId>,
  IdWithCandidates<GradoopId>, TripleWithCandidates<GradoopId>> {
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
  private final TripleWithCandidates<GradoopId> reuseTriple;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public MatchingTriples(final String query) {
    this.query = query;
    this.reuseTriple = new TripleWithCandidates<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = new QueryHandler(query);
  }

  @Override
  public void join(TripleWithSourceEdgeCandidates<GradoopId> pair,
    IdWithCandidates<GradoopId> target,
    Collector<TripleWithCandidates<GradoopId>> collector) throws Exception {

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
