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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TraverserStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.VertexStep;

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
  extends UpdateMapping<K>
  implements FlatJoinFunction<EmbeddingWithTiePoint<K>, VertexStep<K>, EmbeddingWithTiePoint<K>> {
  /**
   * Iteration strategy
   */
  private final TraverserStrategy traverserStrategy;
  /**
   * From field of the next step in the traversal (if not last)
   */
  private int nextFrom;
  /**
   * Index to check in the vertex mapping
   */
  private int vertexCandidate;
  /**
   * Constructor
   *
   * @param tc traversal code for the current exploration
   * @param matchStrategy select if subgraph isomorphism or homomorphism is used
   * @param traverserStrategy iteration strategy
   */
  public UpdateVertexMapping(TraversalCode tc, MatchStrategy matchStrategy,
    TraverserStrategy traverserStrategy) {
    super(tc, matchStrategy);
    this.traverserStrategy = traverserStrategy;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    if (traverserStrategy == TraverserStrategy.SET_PAIR_BULK_ITERATION) {
      setCurrentStepId(getIterationRuntimeContext().getSuperstepNumber() - 1);
    } else if (traverserStrategy == TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION) {
      setCurrentStepId((int) getRuntimeContext().getBroadcastVariable(
        ExplorativePatternMatching.BC_SUPERSTEP).get(0) - 1);
    }
    initializeVisited();

    this.vertexCandidate = (int) getCurrentStep().getTo();
    this.nextFrom = getNextFrom();
  }

  @Override
  public void join(EmbeddingWithTiePoint<K> embedding, VertexStep<K> vertexStep,
    Collector<EmbeddingWithTiePoint<K>> collector) throws Exception {

    K vertexId = vertexStep.getVertexId();
    K[] vertexMapping = embedding.getEmbedding().getVertexMapping();

    if (isValidVertex(vertexId, vertexMapping, vertexCandidate)) {

      vertexMapping[vertexCandidate] = vertexId;
      embedding.getEmbedding().setVertexMapping(vertexMapping);

      if (hasMoreSteps()) {
        embedding.setTiePointId(vertexMapping[nextFrom]);
      }
      collector.collect(embedding);
    }
  }
}
