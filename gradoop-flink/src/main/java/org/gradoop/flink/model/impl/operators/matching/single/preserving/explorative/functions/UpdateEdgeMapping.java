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
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EdgeStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

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
  extends UpdateMapping<K>
  implements FlatJoinFunction<EmbeddingWithTiePoint<K>, EdgeStep<K>, EmbeddingWithTiePoint<K>> {
  /**
   * Iteration strategy
   */
  private final TraverserStrategy traverserStrategy;
  /**
   * Index to check in the edge mapping
   */
  private int edgeCandidate;

  /**
   * Constructor
   *
   * @param traversalCode     traversal code for the current exploration
   * @param matchStrategy select if subgraph isomorphism or homomorphism is used
   * @param traverserStrategy iteration strategy
   */
  public UpdateEdgeMapping(TraversalCode traversalCode,
    MatchStrategy matchStrategy, TraverserStrategy traverserStrategy) {
    super(traversalCode, matchStrategy);
    this.traverserStrategy = traverserStrategy;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // get current step in the traversal
    if (traverserStrategy == TraverserStrategy.SET_PAIR_BULK_ITERATION) {
      setCurrentStepId(getIterationRuntimeContext().getSuperstepNumber() - 1);
    } else if (traverserStrategy == TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION) {
      setCurrentStepId((int) getRuntimeContext().getBroadcastVariable(
        ExplorativePatternMatching.BC_SUPERSTEP).get(0) - 1);
    }
    initializeVisited();

    edgeCandidate = (int) getCurrentStep().getVia();
  }

  @Override
  public void join(EmbeddingWithTiePoint<K> embedding, EdgeStep<K> edgeStep,
    Collector<EmbeddingWithTiePoint<K>> collector) throws Exception {

    K edgeId = edgeStep.getEdgeId();
    K[] edgeMapping = embedding.getEmbedding().getEdgeMapping();

    // traverse if no edge set for that step
    if (isValidEdge(edgeId, edgeMapping, edgeCandidate)) {
      edgeMapping[edgeCandidate] = edgeId;
      embedding.getEmbedding().setEdgeMapping(edgeMapping);
      embedding.setTiePointId(edgeStep.getNextId());
      collector.collect(embedding);
    }
  }
}
