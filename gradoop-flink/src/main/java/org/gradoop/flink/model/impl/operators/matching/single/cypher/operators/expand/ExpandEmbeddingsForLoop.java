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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions.MergeExpandEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandEmbedding;

import java.util.List;

/**
 * Expands an vertex along the edges. The number of hops can be specified via upper and lower bound
 * The input embedding is appended by 2 Entries, the first one represents the path (edge, vertex,
 * edge, vertex, ..., edge), the second one the end vertex
 *
 * Iteration is done with {@code LoopUnrolling}
 */
public class ExpandEmbeddingsForLoop extends ExpandEmbeddings {

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
  public ExpandEmbeddingsForLoop(DataSet<Embedding> input, DataSet<Embedding> candidateEdges,
    int expandColumn, int lowerBound, int upperBound, ExpandDirection direction,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn,
    JoinOperatorBase.JoinHint joinHint) {

    super(input, candidateEdges, expandColumn, lowerBound, upperBound, direction,
      distinctVertexColumns, distinctEdgeColumns, closingColumn, joinHint);
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
  public ExpandEmbeddingsForLoop(DataSet<Embedding> input, DataSet<Embedding> candidateEdges,
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
  public ExpandEmbeddingsForLoop(DataSet<Embedding> input, DataSet<Embedding> candidateEdges,
    int expandColumn, int lowerBound, ExpandDirection direction,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn) {

    this(input, candidateEdges, expandColumn, lowerBound, Integer.MAX_VALUE, direction,
      distinctVertexColumns, distinctEdgeColumns, closingColumn,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);
  }

  @Override
  protected DataSet<ExpandEmbedding> iterate(DataSet<ExpandEmbedding> initialWorkingSet) {
    List<DataSet<ExpandEmbedding>> intermediateResults = Lists.newArrayList(initialWorkingSet);

    for (int i = 1; i < upperBound; i++) {
      DataSet<ExpandEmbedding> nextResult =
        intermediateResults.get(i - 1)
          .join(candidateEdgeTuples, joinHint)
            .where(2).equalTo(0)
            .with(new MergeExpandEmbeddings(distinctVertexColumns, distinctEdgeColumns,
              closingColumn))
            .name("Expand - Expansion Step " + i);

      intermediateResults.add(nextResult);
    }

    int startIndex = Math.max(lowerBound - 1, 0);
    DataSet<ExpandEmbedding> result = intermediateResults.get(startIndex);

    for (int i = startIndex + 1; i < upperBound; i++) {
      result = result.union(intermediateResults.get(i));
    }

    return result;
  }
}

