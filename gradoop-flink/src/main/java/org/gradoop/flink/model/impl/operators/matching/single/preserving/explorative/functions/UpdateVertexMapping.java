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

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.TraverserStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.VertexStep;


import java.util.HashSet;
import java.util.Set;

/**
 * Extends an embedding with a vertex if possible.
 *
 * Read fields first:
 *
 * f1.f0: vertex mappings
 *
 * Read fields second:
 *
 * f0: vertex id
 *
 * Forwarded fields first:
 *
 * f0.f1: edge mappings
 *
 * @param <K> key type
 */
@FunctionAnnotation.ReadFieldsFirst("f1.f0")
@FunctionAnnotation.ReadFieldsSecond("f0")
public class UpdateVertexMapping<K>
  extends RichFlatJoinFunction
    <EmbeddingWithTiePoint<K>, VertexStep<K>, EmbeddingWithTiePoint<K>>
  implements UpdateMapping<K> {
  /**
   * Traversal code
   */
  private final TraversalCode traversalCode;
  /**
   * Match strategy
   */
  private final MatchStrategy matchStrategy;
  /**
   * Iteration strategy
   */
  private final TraverserStrategy traverserStrategy;
  /**
   * Current step in the traversal
   */
  private int currentStepId;
  /**
   * From field of the next step in the traversal (if not last)
   */
  private int nextFrom;
  /**
   * Total number of steps in the traversal
   */
  private int stepCount;
  /**
   * Index to check in the vertex mapping
   */
  private int candidate;
  /**
   * Stores the vertex mapping positions that have been set in previous steps.
   * Needed for isomorphism checks.
   */
  private Set<Integer> previousCandidates;
  /**
   * Constructor
   *
   * @param tc traversal code for the current exploration
   * @param matchStrategy select if subgraph isomorphism or homomorphism is used
   * @param traverserStrategy iteration strategy
   */
  public UpdateVertexMapping(TraversalCode tc, MatchStrategy matchStrategy,
    TraverserStrategy traverserStrategy) {
    this.traversalCode     = tc;
    this.stepCount         = tc.getSteps().size();
    this.matchStrategy     = matchStrategy;
    this.traverserStrategy = traverserStrategy;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    if (traverserStrategy == TraverserStrategy.SET_PAIR_BULK_ITERATION) {
      currentStepId = getIterationRuntimeContext().getSuperstepNumber() - 1;
    } else if (traverserStrategy == TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION) {
      currentStepId = (int) getRuntimeContext().getBroadcastVariable(
        ExplorativePatternMatching.BC_SUPERSTEP).get(0) - 1;
    }

    Step currentStep = traversalCode.getStep(this.currentStepId);
    candidate = (int) currentStep.getTo();

    if (hasMoreSteps()) {
      this.nextFrom = (int) traversalCode
        .getStep(this.currentStepId + 1)
        .getFrom();
    }

    if (matchStrategy == MatchStrategy.ISOMORPHISM) {
      // find previous positions (limited by two times the number steps (edges))
      previousCandidates = new HashSet<>(traversalCode.getSteps().size() * 2);
      for (int i = 0; i < this.currentStepId; i++) {
        Step s = traversalCode.getStep(i);
        previousCandidates.add((int) s.getFrom());
        previousCandidates.add((int) s.getTo());
      }
      // add from field of current step
      previousCandidates.add((int) currentStep.getFrom());
    }
  }

  @Override
  public void join(EmbeddingWithTiePoint<K> embedding, VertexStep<K> vertexStep,
    Collector<EmbeddingWithTiePoint<K>> collector) throws Exception {

    K[] mapping = embedding.getEmbedding().getVertexMapping();
    K id = vertexStep.getVertexId();

    boolean isMapped = mapping[candidate] != null;
    boolean seen = matchStrategy == MatchStrategy.ISOMORPHISM &&
      seenBefore(mapping, id, previousCandidates);

    // not seen before or same as seen before
    if ((!isMapped && !seen) || (isMapped && mapping[candidate].equals(id))) {

      mapping[candidate] = id;
      embedding.getEmbedding().setVertexMapping(mapping);

      // set next tie point if there are more steps in the traversal
      if (hasMoreSteps()) {
        embedding.setTiePointId(mapping[nextFrom]);
      }
      collector.collect(embedding);
    }
  }

  /**
   * Check if there are more traversal steps left.
   *
   * @return true, if there are more steps
   */
  private boolean hasMoreSteps() {
    return currentStepId < stepCount - 1;
  }
}
