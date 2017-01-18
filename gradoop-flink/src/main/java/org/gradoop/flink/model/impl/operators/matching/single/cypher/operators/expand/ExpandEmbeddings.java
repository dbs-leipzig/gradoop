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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.functions
  .ReverseEdgeEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions
  .AdoptEmptyPaths;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions
  .CreateExpandEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions
  .ExtractExpandColumn;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions
  .FilterPreviousExpandEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions
  .MergeExpandEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions
  .PostProcessExpandEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples
  .ExpandEmbedding;

import java.util.List;

/**
 * Expands an vertex along the edges. The number of hops can be specified via upper and lower bound
 * The input embedding is appended by 2 Entries, the first one represents the path (edge, vertex,
 * edge, vertex, ..., edge), the second one the end vertex
 */

public class ExpandEmbeddings implements PhysicalOperator {

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
   * Holds indices of input vertex columns that should be distinct
   */
  private final List<Integer> distinctVertexColumns;
  /**
   * Holds indices of input edge columns that should be distinct
   */
  private final List<Integer> distinctEdgeColumns;
  /**
   * Define the column which should be equal with the paths end
   */
  private final int closingColumn;
  /**
   * join hint
   */
  private final JoinOperatorBase.JoinHint joinHint;

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
  }

  /**
   * New Expand One Operator with default join strategy
   *
   * @param input the embedding which should be expanded
   * @param candidateEdges candidate edges along which we expand
   * @param expandColumn specifies the column that represents the vertex from which we expand
   * @param lowerBound specifies the minimum hops we want to expand
   * @param upperBound specifies the maximum hops we want to expand
   * @param direction direction of the expansion {@see ExpandDirection}
   * @param distinctVertexColumns indices of distinct vertex columns
   * @param distinctEdgeColumns indices of distinct edge columns
   * @param closingColumn defines the column which should be equal with the paths end
   */
  public ExpandEmbeddings(DataSet<Embedding> input, DataSet<Embedding> candidateEdges,
    int expandColumn, int lowerBound, int upperBound, ExpandDirection direction,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn) {

    this(input, candidateEdges, expandColumn, lowerBound, upperBound, direction,
      distinctVertexColumns, distinctEdgeColumns, closingColumn,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);
  }

  /**
   * New Expand One Operator with no upper bound
   *
   * @param input the embedding which should be expanded
   * @param candidateEdges candidate edges along which we expand
   * @param expandColumn specifies the column that represents the vertex from which we expand
   * @param lowerBound specifies the minimum hops we want to expand
   * @param direction direction of the expansion {@see ExpandDirection}
   * @param distinctVertexColumns indices of distinct vertex columns
   * @param distinctEdgeColumns indices of distinct edge columns
   * @param closingColumn defines the column which should be equal with the paths end
   */
  public ExpandEmbeddings(DataSet<Embedding> input, DataSet<Embedding> candidateEdges,
    int expandColumn, int lowerBound, ExpandDirection direction,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn) {

    this(input, candidateEdges, expandColumn, lowerBound, Integer.MAX_VALUE, direction,
      distinctVertexColumns, distinctEdgeColumns, closingColumn,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);
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
   * creates the initial working set from the edge candidates
   *
   * @return initial working set with the expand embeddings
   */
  private DataSet<ExpandEmbedding> preProcess() {
    if (direction == ExpandDirection.IN) {
      candidateEdges = candidateEdges.map(new ReverseEdgeEmbedding());
    } else  if (direction == ExpandDirection.ALL) {
      candidateEdges = candidateEdges.union(candidateEdges.map(new ReverseEdgeEmbedding()));
    }

    return input.join(candidateEdges, joinHint)
      .where(new ExtractExpandColumn(expandColumn))
      .equalTo(new ExtractExpandColumn(0))
      .with(new CreateExpandEmbedding(
        distinctVertexColumns,
        distinctEdgeColumns,
        closingColumn
      ));
  }

  /**
   * Runs the iterative traversal
   *
   * @param initialWorkingSet the initial edges which are used as starting points for the traversal
   * @return set of paths produced by the iteration (length 1..upperBound)
   */
  private DataSet<ExpandEmbedding> iterate(DataSet<ExpandEmbedding> initialWorkingSet) {

    IterativeDataSet<ExpandEmbedding> iteration =
      initialWorkingSet.iterate(upperBound - 1);

    DataSet<ExpandEmbedding> nextWorkingSet = iteration
      .filter(new FilterPreviousExpandEmbedding())
      .join(candidateEdges, joinHint)
        .where(2)
        .equalTo(new ExtractExpandColumn(0))
        .with(new MergeExpandEmbeddings(
          distinctVertexColumns,
          distinctEdgeColumns,
          closingColumn
        ));

    DataSet<ExpandEmbedding> solutionSet = nextWorkingSet.union(iteration);

    return iteration.closeWith(solutionSet, nextWorkingSet);
  }

  /**
   * Produces the final operator results from the iteration results
   *
   * @param iterationResults the results produced by the iteration
   * @return iteration results filtered by upper and lower bound and combined with input data
   */
  private DataSet<Embedding> postProcess(DataSet<ExpandEmbedding> iterationResults) {
    DataSet<Embedding> results =
      iterationResults.flatMap(new PostProcessExpandEmbedding(lowerBound, closingColumn));

    if (lowerBound == 0) {
      results = results.union(
        input.flatMap(new AdoptEmptyPaths(expandColumn, closingColumn))
      );
    }

    return results;
  }
}

