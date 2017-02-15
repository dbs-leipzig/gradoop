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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils;

import com.sun.istack.Nullable;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinOperatorSetsBase;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions.DefaultStringConcatenationFunction;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.Triple;

import java.util.HashSet;

/**
 * Defining some utility functions for the actual join operator implemented with joins
 *
 * Created by Giacomo Bergami on 30/01/17.
 */
public class JoinWithJoinsUtils {
  /**
   * Given two datasets of the same “type” (<code>left</code> and <code>right</code>), join them
   * accordingly to the <code>jointype</code>
   * @param left        Left operand
   * @param right       Right operand
   * @param joinType    Type of join to be used
   * @param <K>         Element type
   * @return            The outcome of the combination of the left and right elements
   */
  public static <K> JoinOperatorSetsBase<K, K> joinByType(
    DataSet<K> left, DataSet<K> right, JoinType  joinType) {
    switch (joinType) {
    case INNER:
      return left.join(right);
    case LEFT_OUTER:
      return left.leftOuterJoin(right);
    case RIGHT_OUTER:
      return left.rightOuterJoin(right);
    default:
      return left.fullOuterJoin(right);
    }
  }

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

  /**
   * Generating the default string concatenator from an user-provided input
   * @param edgeLabelConcatenation  The user defined function for concatenating edges
   * @return                        If the input is null, the DefaultStringConcatenationFunction is
   *                                returned, otherwise the user-defined function.
   */
  public static Function<Tuple2<String, String>, String> generateConcatenator(
    @Nullable Function<Tuple2<String, String>, String> edgeLabelConcatenation) {
    return edgeLabelConcatenation == null ?
      new DefaultStringConcatenationFunction() :
      edgeLabelConcatenation;
  }

  /**
   * Wrapping the user-defined function for EPGMElement comparison with some join equality testing.
   * In the code this function is used for the vertices
   *
   * @param prop  If the element is null, the always true predicate is returned
   * @param <K>   EPGMElement type
   * @return      The extended function
   */
  public static <K extends EPGMElement> Function<Tuple2<K, K>, Boolean> extendBasic
  (@Nullable Function<K, Function<K, Boolean>> prop) {
    return new Function<Tuple2<K, K>, Boolean>() {

      private Function<K, Function<K, Boolean>> local = prop == null ?
        e1 -> e2 -> true : prop;

      @Override
      public Boolean apply(Tuple2<K, K> entity) {
        final K left = entity.f0;
        final K right = entity.f1;
        HashSet<String> ll = new HashSet<String>();
        left.getProperties().getKeys().forEach(ll::add);
        HashSet<String> rr = new HashSet<String>();
        right.getProperties().getKeys().forEach(rr::add);
        ll.retainAll(rr);
        for (String x : ll) {
          if (!left.getPropertyValue(x).equals(right.getPropertyValue(x))) {
            return false;
          }
        }
        return local.apply(left).apply(right);
      }
    };
  }

  /**
   * Wrapping the user-defined function for Triple comparison with some join equality testing.
   * @param prop  If the element is null, the always true predicate is returned
   * @return      The extended function
   */
  public static  Function<Tuple2<Triple, Triple>, Boolean>
  extendBasic2
  (@Nullable Function<Triple, Function<Triple, Boolean>> prop) {
    return new Function<Tuple2<Triple, Triple>, Boolean>() {
      @Override
      public Boolean apply(final Tuple2<Triple, Triple> p) {
        Function<Triple, Function<Triple, Boolean>> local = prop == null ?
          e1 -> e2 -> true : prop;
        HashSet<String> ll = new HashSet<String>();
        p.f0.f1.getProperties().getKeys().forEach(ll::add);
        HashSet<String> rr = new HashSet<String>();
        p.f1.f1.getProperties().getKeys().forEach(rr::add);
        ll.retainAll(rr);
        for (String x : ll) {
          if (!p.f0.f1.getPropertyValue(x).equals(p.f1.f1.getPropertyValue(x))) {
            return false;
          }
        }
        return local.apply(p.f0).apply(p.f1);
      }
    };
  }

}
