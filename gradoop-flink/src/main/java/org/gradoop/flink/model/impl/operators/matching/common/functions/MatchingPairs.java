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
  <IdWithCandidates<GradoopId>, TripleWithCandidates<GradoopId>,
    TripleWithSourceEdgeCandidates<GradoopId>> {
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
  private final TripleWithSourceEdgeCandidates<GradoopId> reuseTuple;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public MatchingPairs(final String query) {
    this.query = query;
    this.reuseTuple = new TripleWithSourceEdgeCandidates<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = new QueryHandler(query);
  }

  @Override
  public void join(IdWithCandidates<GradoopId> sourceVertex,
    TripleWithCandidates<GradoopId> edge,
    Collector<TripleWithSourceEdgeCandidates<GradoopId>> collector)
      throws Exception {

    boolean[] sourceCandidates = sourceVertex.getCandidates();
    boolean[] edgeCandidates   = edge.getCandidates();

    boolean[] newSourceCandidates = new boolean[sourceCandidates.length];
    boolean[] newEdgeCandidates   = new boolean[edgeCandidates.length];

    boolean pairStillValid = false;

    for (int eQ = 0; eQ < edgeCandidates.length; eQ++) {
      if (edgeCandidates[eQ]) {
        int vQ = queryHandler
          .getEdgeById((long) eQ).getSourceVertexId().intValue();
        if (sourceCandidates[vQ]) {
          newSourceCandidates[vQ] = true;
          newEdgeCandidates[eQ]   = true;
          pairStillValid          = true;
        }
      }
    }

    if (pairStillValid) {
      reuseTuple.setEdgeId(edge.getEdgeId());
      reuseTuple.setSourceId(edge.getSourceId());
      reuseTuple.setSourceCandidates(newSourceCandidates);
      reuseTuple.setTargetId(edge.getTargetId());
      reuseTuple.setEdgeCandidates(newEdgeCandidates);
      collector.collect(reuseTuple);
    }
  }
}
