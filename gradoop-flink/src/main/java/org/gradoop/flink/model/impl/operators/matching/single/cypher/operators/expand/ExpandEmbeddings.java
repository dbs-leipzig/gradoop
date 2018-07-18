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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.ReverseEdgeEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions.AdoptEmptyPaths;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions.CreateExpandEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions.ExtractExpandColumn;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions.ExtractKeyedCandidateEdges;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions.PostProcessExpandEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.EdgeWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandEmbedding;

import java.util.List;

/**
 * Expands an vertex along the edges. The number of hops can be specified via upper and lower bound
 * The input embedding is appended by 2 Entries, the first one represents the path (edge, vertex,
 * edge, vertex, ..., edge), the second one the end vertex
 */
public abstract class ExpandEmbeddings implements PhysicalOperator {
  /**
   * Input Embeddings
   */
  protected final DataSet<Embedding> input;
  /**
   * specifies the input column that will be expanded
   */
  protected final int expandColumn;
  /**
   * minimum hops
   */
  protected final int lowerBound;
  /**
   * maximum hops
   */
  protected final int upperBound;
  /**
   * expand direction
   */
  protected final ExpandDirection direction;
  /**
   * Holds indices of input vertex columns that should be distinct
   */
  protected final List<Integer> distinctVertexColumns;
  /**
   * Holds indices of input edge columns that should be distinct
   */
  protected final List<Integer> distinctEdgeColumns;
  /**
   * Define the column which should be equal with the paths end
   */
  protected final int closingColumn;
  /**
   * join hint
   */
  protected final JoinOperatorBase.JoinHint joinHint;
  /**
   * Candidate edges
   */
  protected DataSet<Embedding> candidateEdges;
  /**
   * candidate edges with extracted map key
   */
  protected DataSet<EdgeWithTiePoint> candidateEdgeTuples;

  /**
   * Operator name used for Flink operator description
   */
  protected String name;

  /**
   * New Expand One Operator
   *
   * @param input the embedding which should be expanded
   * @param candidateEdges candidate edges along which we expand
   * @param expandColumn specifies the input column that represents the vertex from which we expand
   * @param lowerBound specifies the minimum hops we want to expand
   * @param upperBound specifies the maximum hops we want to expand
   * @param direction direction of the expansion {@see ExpandDirection}
   * @param distinctVertexColumns indices of distinct input vertex columns
   * @param distinctEdgeColumns indices of distinct input edge columns
   * @param closingColumn defines the column which should be equal with the paths end
   * @param joinHint join strategy
   */
  public ExpandEmbeddings(DataSet<Embedding> input, DataSet<Embedding> candidateEdges,
    int expandColumn, int lowerBound, int upperBound, ExpandDirection direction,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn,
    JoinOperatorBase.JoinHint joinHint) {

    this.input = input;
    this.candidateEdges = candidateEdges;
    this.expandColumn = expandColumn;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.direction = direction;
    this.distinctVertexColumns = distinctVertexColumns;
    this.distinctEdgeColumns = distinctEdgeColumns;
    this.closingColumn = closingColumn;
    this.joinHint = joinHint;
    this.setName("ExpandEmbeddings");
  }

  /**
   * Runs a traversal over the given edgeCandidates withing the given bounds
   *
   * @return the input appened by 2 entries (IdList(Path), IdEntry(End Vertex)
   */
  @Override
  public DataSet<Embedding> evaluate() {
    DataSet<ExpandEmbedding> initialWorkingSet = preProcess();

    DataSet<ExpandEmbedding> iterationResults = iterate(initialWorkingSet);

    return postProcess(iterationResults);
  }

  /**
   * Runs the iterative traversal
   *
   * @param initialWorkingSet the initial edges which are used as starting points for the traversal
   * @return set of paths produced by the iteration (length 1..upperBound)
   */
  protected abstract DataSet<ExpandEmbedding> iterate(DataSet<ExpandEmbedding> initialWorkingSet);

  /**
   * creates the initial working set from the edge candidates
   *
   * @return initial working set with the expand embeddings
   */
  private DataSet<ExpandEmbedding> preProcess() {
    if (direction == ExpandDirection.IN) {
      candidateEdges = candidateEdges
        .map(new ReverseEdgeEmbedding())
        .name(getName() + " - Reverse Edges");
    }

    this.candidateEdgeTuples = candidateEdges
      .map(new ExtractKeyedCandidateEdges())
      .name(getName() + " - Create candidate edge tuples")
      .partitionByHash(0)
      .name(getName() + " - Partition edge tuples");

    return input.join(candidateEdgeTuples, joinHint)
      .where(new ExtractExpandColumn(expandColumn)).equalTo(0)
      .with(new CreateExpandEmbedding(
        distinctVertexColumns,
        distinctEdgeColumns,
        closingColumn
      ))
      .name(getName() + " - Initial expansion");
  }

  /**
   * Produces the final operator results from the iteration results
   *
   * @param iterationResults the results produced by the iteration
   * @return iteration results filtered by upper and lower bound and combined with input data
   */
  private DataSet<Embedding> postProcess(DataSet<ExpandEmbedding> iterationResults) {
    DataSet<Embedding> results = iterationResults
      .flatMap(new PostProcessExpandEmbedding(lowerBound, closingColumn))
      .name(getName() + " - Post Processing");

    if (lowerBound == 0) {
      results = results.union(
        input
          .flatMap(new AdoptEmptyPaths(expandColumn, closingColumn))
          .name(getName() + " - Append empty paths")
      );
    }

    return results;
  }

  @Override
  public void setName(String newName) {
    this.name = newName;
  }

  @Override
  public String getName() {
    return this.name;
  }
}
