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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.physical_operators;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;

/**
 * Expands an Edge along the edges. The number of hops can be specified via upper and lower bound
 * The input embedding is appended by 2 Entries, the first one represents the path, the second one the
 * end vertex
 */
public class Expand implements PhysicalOperator {

  private final DataSet<Embedding> input;
  private final DataSet<Embedding> candidateEdges;
  private final int expandColumn;
  private final int lowerBound;
  private final int upperBound;
  private final ExpandDirection direction;
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
   * @param joinHint join strategy
   */
  public Expand(DataSet<Embedding> input, DataSet<Embedding> candidateEdges, int expandColumn,
    int lowerBound, int upperBound, ExpandDirection direction, JoinOperatorBase.JoinHint joinHint) {
    this.input = input;
    this.candidateEdges = candidateEdges;
    this.expandColumn = expandColumn;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.direction = direction;
    this.joinHint = joinHint;
  }

  public Expand(DataSet<Embedding> input, DataSet<Embedding> candidateEdges, int expandColumn,
    int lowerBound, int upperBound, ExpandDirection direction) {

    this(input, candidateEdges, expandColumn, lowerBound, upperBound, direction,
      JoinOperatorBase.JoinHint .BROADCAST_HASH_FIRST);
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return null;
  }
}
