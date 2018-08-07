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
