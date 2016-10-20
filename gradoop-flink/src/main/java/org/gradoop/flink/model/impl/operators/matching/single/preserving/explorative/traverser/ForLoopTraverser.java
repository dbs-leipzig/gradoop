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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
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
 * For the iteration the traverser uses a basic for loop.
 *
 * @param <K> key type
 */
public class ForLoopTraverser<K> extends DistributedTraverser<K> {

  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode describes the traversal through the graph
   * @param vertexCount   number of query vertices
   * @param edgeCount     number of query edges
   * @param keyClazz      needed for embedding initialization
   */
  public ForLoopTraverser(TraversalCode traversalCode,
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
  public ForLoopTraverser(TraversalCode traversalCode,
    MatchStrategy matchStrategy,
    int vertexCount, int edgeCount,
    Class<K> keyClazz,
    JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    JoinOperatorBase.JoinHint vertexStepJoinStrategy,
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping) {
    super(traversalCode, matchStrategy, vertexCount, edgeCount, keyClazz,
      edgeStepJoinStrategy, vertexStepJoinStrategy, vertexMapping, edgeMapping);
  }

  @Override
  public DataSet<Tuple1<Embedding<K>>> traverse(
    DataSet<IdWithCandidates<K>> vertices,
    DataSet<TripleWithCandidates<K>> edges) {
    return iterate(vertices, edges, buildInitialEmbeddings(vertices))
      .project(1);
  }

  /**
   * Explores the data graph iteratively using the provided traversal code.
   *
   * @param vertices   vertex candidates
   * @param edges      edge candidates
   * @param embeddings initial embeddings which are extended in each iteration
   * @return final embeddings
   */
  private DataSet<EmbeddingWithTiePoint<K>> iterate(
    DataSet<IdWithCandidates<K>> vertices,
    DataSet<TripleWithCandidates<K>> edges,
    DataSet<EmbeddingWithTiePoint<K>> embeddings) {

    TraversalCode traversalCode = getTraversalCode();

    for (int i = 0; i < traversalCode.getSteps().size(); i++) {
      Step step = traversalCode.getStep(i);

      DataSet<Integer> superstep = vertices.getExecutionEnvironment()
        .fromElements(i + 1);

      String[] forwardedFieldEdgeSteps = new String[] {
          // forward edge id
          "f0",
          // forward source or target id to tie point id
          step.isOutgoing() ? "f1->f1" : "f2->f1",
          // forward source or target id to next id
          step.isOutgoing() ? "f2->f2" : "f1->f2"
      };

      embeddings = traverseEdges(edges, embeddings, superstep,
        IterationStrategy.LOOP_UNROLLING, forwardedFieldEdgeSteps);
      embeddings = traverseVertices(vertices, embeddings, superstep,
        IterationStrategy.LOOP_UNROLLING);
    }

    return embeddings;
  }
}
