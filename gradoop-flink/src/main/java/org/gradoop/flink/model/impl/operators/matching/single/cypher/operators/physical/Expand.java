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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.CombineExpandEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.CreateInitialExpandEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.FilterExpandResultByLowerBound;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.FilterOldExpandIterationResults;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.ProduceExpandResult;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.ReverseEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.EmbeddingKeySelector;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;

import java.util.List;


/**
 * Expands an vertex along the edges. The number of hops can be specified via upper and lower bound
 * The input embedding is appended by 2 Entries, the first one represents the path,
 * the second one the end vertex
 */

public class Expand implements PhysicalOperator {

  /**
   * Input Embeddings
   */
  private final DataSet<Embedding> input;
  /**
   * Candidate edges
   */
  private DataSet<Embedding> candidateEdges;
  /**
   * specifies the input column that will be expanded
   */
  private final int expandColumn;
  /**
   * minimum hops
   */
  private final int lowerBound;
  /**
   * maximum hops
   */
  private final int upperBound;
  /**
   * expand direction
   */
  private final ExpandDirection direction;
  /**
   * Holds indices of vertex columns that should be distinct
   */
  private final List<Integer> distinctVertexColumns;
  /**
   * Holds indices of edge columns that should be distinct
   */
  private final List<Integer> distinctEdgeColumns;
  /**
   * join hint
   */
  private final JoinOperatorBase.JoinHint joinHint;


  /**
   * New Expand One Operator
   *
   * @param input the embedding which should be expanded
   * @param candidateEdges candidate edges along which we expand
   * @param expandColumn specifies the colum that represents the vertex from which we expand
   * @param lowerBound specifies the minimum hops we want to expand
   * @param upperBound specifies the maximum hops we want to expand
   * @param direction direction of the expansion {@see ExpandDirection}
   * @param distinctVertexColumns indices of distinct vertex columns
   * @param distinctEdgeColumns indices of distinct edge columns
   * @param joinHint join strategy
   */
  public Expand(DataSet<Embedding> input, DataSet<Embedding> candidateEdges, int expandColumn,
    int lowerBound, int upperBound, ExpandDirection direction,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns,
    JoinOperatorBase.JoinHint joinHint) {

    this.input = input;
    this.candidateEdges = candidateEdges;
    this.expandColumn = expandColumn;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.direction = direction;
    this.distinctVertexColumns = distinctVertexColumns;
    this.distinctEdgeColumns = distinctEdgeColumns;
    this.joinHint = joinHint;
  }

  /**
   * New Expand One Operator with default join strategy
   *
   * @param input the embedding which should be expanded
   * @param candidateEdges candidate edges along which we expand
   * @param expandColumn specifies the colum that represents the vertex from which we expand
   * @param lowerBound specifies the minimum hops we want to expand
   * @param upperBound specifies the maximum hops we want to expand
   * @param direction direction of the expansion {@see ExpandDirection}
   * @param distinctVertexColumns indices of distinct vertex columns
   * @param distinctEdgeColumns indices of distinct edge columns
   */
  public Expand(DataSet<Embedding> input, DataSet<Embedding> candidateEdges, int expandColumn,
    int lowerBound, int upperBound, ExpandDirection direction,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns) {

    this.input = input;
    this.candidateEdges = candidateEdges;
    this.expandColumn = expandColumn;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.direction = direction;
    this.distinctVertexColumns = distinctVertexColumns;
    this.distinctEdgeColumns = distinctEdgeColumns;
    this.joinHint = JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES;
  }

  /**
   * Runs a traversel over the given edgeCandidates withing the given bounds
   * @return the input appened by 2 entries (IdList(Path), IdEntry(End Vertex)
   */
  @Override
  public DataSet<Embedding> evaluate() {
    DataSet<Embedding> initialWorkingSet = preprocess();

    DataSet<Embedding> iterationResults = iterate(initialWorkingSet);

    return postprocess(iterationResults);
  }

  /**
   * creates the initial working set from the edge candidates
   * @return initial working set with the expand embeddings
   */
  private DataSet<Embedding> preprocess() {
    if (direction == ExpandDirection.IN) {
      candidateEdges = candidateEdges.map(new ReverseEmbeddings());
    }

    return input.join(candidateEdges, joinHint)
      .where(new EmbeddingKeySelector(expandColumn))
      .equalTo(new EmbeddingKeySelector(0))
      .with(new CreateInitialExpandEmbedding(distinctVertexColumns, distinctEdgeColumns));
  }

  /**
   * Runs the iterative traversal
   * @param initialWorkingSet the initial edges which are used as starting points for the traversal
   * @return set of paths produced by the iteration (length 1..upperBound)
   */
  private DataSet<Embedding> iterate(DataSet<Embedding> initialWorkingSet) {
    IterativeDataSet<Embedding> iteration = initialWorkingSet.iterate(upperBound - 1);

    DataSet<Embedding> nextWorkingSet = iteration
      .filter(new FilterOldExpandIterationResults())
      .join(candidateEdges, joinHint)
        .where(new EmbeddingKeySelector(EmbeddingKeySelector.LAST))
        .equalTo(new EmbeddingKeySelector(0))
        .with(new CombineExpandEmbeddings(distinctVertexColumns, distinctEdgeColumns));

    DataSet<Embedding> solutionSet = nextWorkingSet.union(iteration);

    return iteration.closeWith(solutionSet, nextWorkingSet);
  }

  /**
   * Produces the final operator results from the iteration results
   * @param iterationResults the results produced by the iteration
   * @return iteration results filtered by upper and lower bound and combined with input data
   */
  private DataSet<Embedding> postprocess(DataSet<Embedding> iterationResults) {
    iterationResults = iterationResults.filter(new FilterExpandResultByLowerBound(lowerBound));

    DataSet<Embedding> results =  input.join(iterationResults)
      .where(new EmbeddingKeySelector(expandColumn))
      .equalTo(new EmbeddingKeySelector(0))
      .with(new ProduceExpandResult());

    if (lowerBound == 0) {
      results = results.union(input);
    }

    return results;
  }
}

