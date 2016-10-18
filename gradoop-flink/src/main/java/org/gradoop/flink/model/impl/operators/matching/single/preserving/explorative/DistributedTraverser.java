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

package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.functions.utils.Superstep;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.functions.ElementHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug.PrintEdgeStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug.PrintEmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug.PrintVertexStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.BuildEdgeStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.BuildEmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.BuildVertexStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.EdgeHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.UpdateEdgeMappings;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.UpdateVertexMappings;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.VertexHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EdgeStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.VertexStep;

import java.util.Objects;

import static org.gradoop.flink.model.impl.operators.matching.common.debug.Printer.log;

/**
 * Extracts {@link Embedding}s iteratively from a given graph by traversing the
 * graph according to a given {@link TraversalCode}.
 *
 * @param <K> key type
 */
public class DistributedTraverser<K> {
  /**
   * Defines the traversal order
   */
  private final TraversalCode traversalCode;
  /**
   * Number of vertices in the query graph.
   */
  private final int vertexCount;
  /**
   * Number of edges in the query graph.
   */
  private final int edgeCount;
  /**
   * Join strategy used for the join between embeddings and edges
   */
  private final JoinOperatorBase.JoinHint edgeStepJoinStrategy;
  /**
   * Join strategy used for the join between embeddings and vertices
   */
  private final JoinOperatorBase.JoinHint vertexStepJoinStrategy;
  /**
   * Vertex mapping used for debug
   */
  private final DataSet<Tuple2<K, PropertyValue>> vertexMapping;
  /**
   * Edge mapping used for debug
   */
  private final DataSet<Tuple2<K, PropertyValue>> edgeMapping;
  /**
   *
   */
  private final MatchStrategy matchStrategy;

  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode describes the traversal through the graph\
   * @param vertexCount number of query vertices
   * @param edgeCount number of query edges
   */
  public DistributedTraverser(TraversalCode traversalCode,
    int vertexCount, int edgeCount) {
    this(traversalCode,
      vertexCount, edgeCount,
      null, null, // debug mappings
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES,
      MatchStrategy.ISOMORPHISM);
  }

  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode           describes the traversal through the graph
   * @param vertexCount             number of query vertices
   * @param edgeCount               number of query edges
   * @param vertexMapping           used for debug
   * @param edgeMapping             used for debug
   * @param edgeStepJoinStrategy    {@link JoinOperatorBase.JoinHint} edges
   * @param vertexStepJoinStrategy  {@link JoinOperatorBase.JoinHint} vertices
   * @param matchStrategy  {@link MatchStrategy} used for pattern matching
   */
  public DistributedTraverser(TraversalCode traversalCode,
    int vertexCount, int edgeCount,
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping,
    JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    JoinOperatorBase.JoinHint vertexStepJoinStrategy,
    MatchStrategy matchStrategy) {
    Objects.requireNonNull(traversalCode);
    Objects.requireNonNull(edgeStepJoinStrategy);
    Objects.requireNonNull(vertexStepJoinStrategy);
    this.traversalCode          = traversalCode;
    this.vertexCount            = vertexCount;
    this.edgeCount              = edgeCount;
    this.vertexMapping          = vertexMapping;
    this.edgeMapping            = edgeMapping;
    this.edgeStepJoinStrategy   = edgeStepJoinStrategy;
    this.vertexStepJoinStrategy = vertexStepJoinStrategy;
    this.matchStrategy = matchStrategy;
  }

  /**
   * Traverses the graph according to the provided traversal code.
   *
   * @param keyClazz needed for embedding initialization
   * @param vertices vertices including their query candidates
   * @param edges    edges including their query candidates
   * @return found embeddings
   */
  public DataSet<Tuple1<Embedding<K>>> traverse(
    Class<K> keyClazz,
    DataSet<IdWithCandidates<K>> vertices,
    DataSet<TripleWithCandidates<K>> edges) {

    return iterate(vertices, edges, buildInitialEmbeddings(keyClazz, vertices))
      .project(1);
  }

  /**
   * Builds the initial embeddings from the given vertices.
   *
   * @param keyClass used for embedding array initialization
   * @param vertices vertices and their query candidates
   * @return initial embeddings
   */
  private DataSet<EmbeddingWithTiePoint<K>> buildInitialEmbeddings(
    Class<K> keyClass,
    DataSet<IdWithCandidates<K>> vertices) {

    DataSet<EmbeddingWithTiePoint<K>> initialEmbeddings = vertices
      .filter(new ElementHasCandidate<>(traversalCode.getStep(0).getFrom()))
      .map(new BuildEmbeddingWithTiePoint<>(keyClass, traversalCode,
        vertexCount, edgeCount));

    return log(initialEmbeddings, new PrintEmbeddingWithTiePoint<>(),
      vertexMapping, edgeMapping);
  }

  /**
   * Explores the data graph according to the traversal code of this operator.
   *
   * @param vertices          vertex candidates
   * @param edges             edge candidates
   * @param initialEmbeddings initial embeddings which are extended
   * @return final embeddings
   */
  private DataSet<EmbeddingWithTiePoint<K>> iterate(
    DataSet<IdWithCandidates<K>> vertices,
    DataSet<TripleWithCandidates<K>> edges,
    DataSet<EmbeddingWithTiePoint<K>> initialEmbeddings) {

    // ITERATION HEAD
    IterativeDataSet<EmbeddingWithTiePoint<K>> embeddings = initialEmbeddings
      .iterate(traversalCode.getSteps().size());

    // ITERATION BODY

    // get current superstep
    DataSet<Integer> superstep = embeddings
      .first(1)
      .map(new Superstep<>());

    // traverse to outgoing/incoming edges
    DataSet<EdgeStep<K>> edgeSteps = edges
      .filter(new EdgeHasCandidate<>(traversalCode))
      .withBroadcastSet(superstep, ExplorativePatternMatching.BC_SUPERSTEP)
      .map(new BuildEdgeStep<>(traversalCode))
      .withBroadcastSet(superstep, ExplorativePatternMatching.BC_SUPERSTEP);

    edgeSteps = log(edgeSteps,
      new PrintEdgeStep<>(true, "post-filter-map-edge"),
      vertexMapping, edgeMapping);

    DataSet<EmbeddingWithTiePoint<K>> nextWorkSet = embeddings
      .join(edgeSteps, edgeStepJoinStrategy)
      .where(0).equalTo(1) // tiePointId == sourceId/targetId tie point
      .with(new UpdateEdgeMappings<>(traversalCode));

    nextWorkSet = log(nextWorkSet,
      new PrintEmbeddingWithTiePoint<>(true, "post-edge-update"),
      vertexMapping, edgeMapping);

    // traverse to vertices
    DataSet<VertexStep<K>> vertexSteps = vertices
      .filter(new VertexHasCandidate<>(traversalCode))
      .withBroadcastSet(superstep, ExplorativePatternMatching.BC_SUPERSTEP)
      .map(new BuildVertexStep<>());

    vertexSteps = log(vertexSteps,
      new PrintVertexStep<>(true, "post-filter-project-vertex"),
      vertexMapping, edgeMapping);

    nextWorkSet = nextWorkSet
      .join(vertexSteps, vertexStepJoinStrategy)
      .where(0).equalTo(0) // tiePointId == vertexId
      .with(new UpdateVertexMappings<>(traversalCode, matchStrategy));

    nextWorkSet = log(nextWorkSet,
      new PrintEmbeddingWithTiePoint<>(true, "post-vertex-update"),
      vertexMapping, edgeMapping);

    // ITERATION FOOTER
    return embeddings.closeWith(nextWorkSet, nextWorkSet);
  }


}
