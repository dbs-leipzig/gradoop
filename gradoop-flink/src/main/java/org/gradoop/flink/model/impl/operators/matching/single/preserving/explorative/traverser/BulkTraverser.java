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

package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.functions.utils.Superstep;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.IterationStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

/**
 * Extracts {@link Embedding}s iteratively from a given graph by traversing the
 * graph according to a given {@link TraversalCode}.
 *
 * For the iteration the traverser uses Flink bulk iteration.
 *
 * @param <K> key type
 */
public class BulkTraverser<K> extends DistributedTraverser<K> {
  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode describes the traversal through the graph
   * @param vertexCount   number of query vertices
   * @param edgeCount     number of query edges
   * @param keyClazz      needed for embedding initialization
   */
  public BulkTraverser(TraversalCode traversalCode,
    int vertexCount, int edgeCount, Class<K> keyClazz) {
    this(traversalCode, MatchStrategy.ISOMORPHISM,
      vertexCount, edgeCount,
      keyClazz,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES,
      null, null); // debug mappings
  }

  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode          describes the graph traversal
   * @param matchStrategy          matching strategy for vertex and edge mapping
   * @param vertexCount            number of query vertices
   * @param edgeCount              number of query edges
   * @param keyClazz               key type for embedding initialization
   * @param edgeStepJoinStrategy   Join strategy for edge extension
   * @param vertexStepJoinStrategy Join strategy for vertex extension
   * @param vertexMapping          used for debug
   * @param edgeMapping            used for debug
   */
  public BulkTraverser(TraversalCode traversalCode, MatchStrategy matchStrategy,
    int vertexCount, int edgeCount,
    Class<K> keyClazz,
    JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    JoinOperatorBase.JoinHint vertexStepJoinStrategy,
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping) {

    super(traversalCode, matchStrategy, vertexCount, edgeCount, keyClazz,
      edgeStepJoinStrategy, vertexStepJoinStrategy, vertexMapping, edgeMapping);
  }

  /**
   * Traverses the graph according to the provided traversal code.
   *
   * @param vertices vertices including their query candidates
   * @param edges    edges including their query candidates
   * @return found embeddings
   */
  @Override
  public DataSet<Tuple1<Embedding<K>>> traverse(
    DataSet<IdWithCandidates<K>> vertices,
    DataSet<TripleWithCandidates<K>> edges) {

    return iterate(vertices, edges, buildInitialEmbeddings(vertices))
      .project(1);
  }

  @Override
  boolean isIterative() {
    return true;
  }

  /**
   * Explores the data graph iteratively using the provided traversal code.
   *
   * @param vertices          vertex candidates
   * @param edges             edge candidates
   * @param initialEmbeddings initial embeddings which are extended in each
   *                          iteration
   * @return final embeddings
   */
  private DataSet<EmbeddingWithTiePoint<K>> iterate(
    DataSet<IdWithCandidates<K>> vertices,
    DataSet<TripleWithCandidates<K>> edges,
    DataSet<EmbeddingWithTiePoint<K>> initialEmbeddings) {

    // ITERATION HEAD
    IterativeDataSet<EmbeddingWithTiePoint<K>> embeddings = initialEmbeddings
      .iterate(getTraversalCode().getSteps().size());

    // ITERATION BODY

    // get current superstep
    DataSet<Integer> superstep = embeddings
      .first(1)
      .map(new Superstep<>());

    // traverse to outgoing/incoming edges
    String[] forwardedFieldsEdgeSteps = new String[] {
        "f0" // forward edge id
    };
    DataSet<EmbeddingWithTiePoint<K>> nextWorkSet = traverseEdges(edges,
      embeddings, superstep, IterationStrategy.BULK_ITERATION,
      forwardedFieldsEdgeSteps);

    // traverse to vertices
    nextWorkSet = traverseVertices(vertices, nextWorkSet, superstep,
      IterationStrategy.BULK_ITERATION);

    // ITERATION FOOTER
    return embeddings.closeWith(nextWorkSet, nextWorkSet);
  }
}
