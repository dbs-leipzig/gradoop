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

package org.gradoop.flink.model.impl.operators.join;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinOperatorSetsBase;
import org.apache.flink.api.java.operators.join.JoinType;

/**
 * Created by vasistas on 16/02/17.
 */
public class JoinUtils {
  /**
   * Joins an operand dataset with a disambiguation one
   *
   * @param left            Operand
   * @param right           Disambiguation dataset
   * @param joinType        Type of join to be used
   * @param isCorrectOrder  Checks if the left is actually the left operand and not the right one.
   *                        The right operand, in this case, is always the disambiguation dataset
   * @param <K>             Left element type
   * @param <J>             Right element type
   * @return                The outcome of the combination of the left and right elements
   */
  public static <K, J> JoinOperatorSetsBase<K, J> joinByVertexEdge(DataSet<K> left,
    DataSet<J> right, JoinType joinType, boolean isCorrectOrder) {
    switch (joinType) {
    case INNER:
      return left.join(right);
    case LEFT_OUTER:
      return isCorrectOrder ? left.leftOuterJoin(right) : left.join(right);
    case RIGHT_OUTER:
      return isCorrectOrder ? left.join(right) : left.leftOuterJoin(right);
    default:
      return left.fullOuterJoin(right);
    }
  }
}
