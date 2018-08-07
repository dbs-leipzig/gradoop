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
