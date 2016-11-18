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
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;

/**
 * Expands a vertex along the edges by one hop in the given direction
 * The input embedding is appended by 2 entries
 */
public class ExpandOne implements PhysicalOperator {

  /**
   * Input embeddings
   */
  private final DataSet<Embedding> input;
  /**
   * Edge candidates along which we expand
   */
  private final DataSet<Embedding> candidateEdges;
  /**
   * Column of the input embedding which we will expand
   */
  private final int expandColumn;
  /**
   * Expand direction
   */
  private final ExpandDirection direction;
  /**
   * The strategy used for the matching
   */
  private final MatchStrategy matchStrategy;
  /**
   * Join Hint
   */
  private final JoinOperatorBase.JoinHint joinHint;

  /**
   * New ExpandOne operator
   *
   * @param input the embedding which should be expanded
   * @param candidateEdges candidate edges along which we expand
   * @param expandColumn specifies the colum that represents the vertex from which we expand
   * @param direction direction of the expansion {@see ExpandDirection}
   * @param matchStrategy match strategy
   * @param joinHint join strategy
   */
  public ExpandOne(DataSet<Embedding> input, DataSet<Embedding> candidateEdges, int expandColumn,
    ExpandDirection direction, MatchStrategy matchStrategy, JoinOperatorBase.JoinHint joinHint) {
    this.input = input;
    this.candidateEdges = candidateEdges;
    this.expandColumn = expandColumn;
    this.direction = direction;
    this.matchStrategy = matchStrategy;
    this.joinHint = joinHint;
  }

  /**
   * New ExpandOne operator with default join hint
   *
   * @param input the embedding which should be expanded
   * @param candidateEdges candidate edges along which we expand
   * @param expandColumn specifies the colum that represents the vertex from which we expand
   * @param direction direction of the expansion {@see ExpandDirection}
   */
  public ExpandOne(DataSet<Embedding> input, DataSet<Embedding> candidateEdges, int expandColumn,
    ExpandDirection direction) {

    this(input, candidateEdges, expandColumn, direction, MatchStrategy.ISOMORPHISM,
      JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST);
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return null;
  }
}
