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
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.TraverserStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EdgeStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

import java.util.ArrayList;
import java.util.List;

/**
 * Extends an embedding with an edge if possible.
 *
 * Read fields first:
 *
 * f1.f1: edge mappings
 *
 * Read fields second:
 *
 * f0: edge id
 * f2: next id
 *
 * Forwarded fields first:
 *
 * f1.f0: vertex mappings
 *
 * Forwarded fields second:
 *
 * f2->f1: next id -> tie point id
 *
 * @param <K> key type
 */
@FunctionAnnotation.ReadFieldsFirst("f1.f1")
@FunctionAnnotation.ReadFieldsSecond("f0;f2")
@FunctionAnnotation.ForwardedFieldsFirst("f1.f0")
@FunctionAnnotation.ForwardedFieldsSecond("f2->f0")
public class UpdateEdgeMapping<K>
  extends RichFlatJoinFunction
    <EmbeddingWithTiePoint<K>, EdgeStep<K>, EmbeddingWithTiePoint<K>>
  implements UpdateMapping<K> {
  /**
   * Traversal code for the current exploration
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
   * Index to check in the edge mapping
   */
  private int candidate;
  /**
   * List of edge candidates that have been previously processed. This is needed
   * for isomorphism checks.
   */
  private List<Integer> previousCandidates;

  /**
   * Constructor
   *
   * @param traversalCode     traversal code for the current exploration
   * @param matchStrategy select if subgraph isomorphism or homomorphism is used
   * @param traverserStrategy iteration strategy
   */
  public UpdateEdgeMapping(TraversalCode traversalCode,
    MatchStrategy matchStrategy, TraverserStrategy traverserStrategy) {
    this.traversalCode      = traversalCode;
    this.matchStrategy      = matchStrategy;
    this.traverserStrategy = traverserStrategy;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    int currentStep = 0;
    // get current step in the traversal
    if (traverserStrategy == TraverserStrategy.SET_PAIR_BULK_ITERATION) {
      currentStep = getIterationRuntimeContext().getSuperstepNumber() - 1;
    } else if (traverserStrategy == TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION) {
      currentStep = (int) getRuntimeContext().getBroadcastVariable(
        ExplorativePatternMatching.BC_SUPERSTEP).get(0) - 1;
    }

    candidate = (int) traversalCode.getStep(currentStep).getVia();

    // get previous candidates
    if (matchStrategy == MatchStrategy.ISOMORPHISM) {
      previousCandidates = new ArrayList<>(currentStep);
      for (int i = 0; i < currentStep; i++) {
        previousCandidates.add((int) traversalCode.getStep(i).getVia());
      }
    }
  }

  @Override
  public void join(EmbeddingWithTiePoint<K> embedding, EdgeStep<K> edgeStep,
    Collector<EmbeddingWithTiePoint<K>> collector) throws Exception {

    K[] mapping = embedding.getEmbedding().getEdgeMapping();
    K id = edgeStep.getEdgeId();

    boolean isMapped = mapping[candidate] != null;
    boolean seen = matchStrategy == MatchStrategy.ISOMORPHISM &&
      seenBefore(mapping, id, previousCandidates);

    // traverse if no edge set for that step
    if (!isMapped && !seen) {
      mapping[candidate] = id;
      embedding.getEmbedding().setEdgeMapping(mapping);
      embedding.setTiePointId(edgeStep.getNextId());
      collector.collect(embedding);
    }
  }
}
