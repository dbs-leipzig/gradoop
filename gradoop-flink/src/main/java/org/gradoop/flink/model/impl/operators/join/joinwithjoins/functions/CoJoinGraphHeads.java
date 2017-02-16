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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;

/**
 *
 * Defines the way to define a new graph head from two other ones
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class CoJoinGraphHeads  implements CoGroupFunction<GraphHead, GraphHead, GraphHead>,
  Serializable {

  /**
   * Basic graph head that could be returned when an empty graph should be returned (no matching
   * graphs)
   */
  private static final GraphHead BASIC = new GraphHead();

  /**
   * Function describing if the two graph heads match or not.
   */
  private final Function<Tuple2<GraphHead, GraphHead>, Boolean> thetaGraph;
  /**
   * Function for combining the matching graph heads together
   */
  private final Oplus<GraphHead> combineHeads;

  /**
   * Reusable tuple for many times within the worker
   */
  private final Tuple2<GraphHead, GraphHead> tuple2;

  /**
   * Graph id to which the GraphHead belongs to
   */
  private GradoopId gid;

  /**
   * Default constructor
   * @param thetaGraph      Function for selecting graph heads
   * @param combineHeads    Way to combine two graph heads
   */
  public CoJoinGraphHeads(Function<Tuple2<GraphHead, GraphHead>, Boolean> thetaGraph,
    Oplus<GraphHead> combineHeads) {
    this.thetaGraph = thetaGraph;
    this.combineHeads = combineHeads;
    this.gid = null;
    this.tuple2 = new Tuple2<>();
  }

  /**
   * Setting the id of the graph that has to be returned
   * @param id    The id of the graph that has to be returned
   * @return      An updated instance of the same class
   */
  public CoJoinGraphHeads setGraphId(GradoopId id) {
    this.gid = id;
    return this;
  }

  @Override
  public void coGroup(Iterable<GraphHead> first, Iterable<GraphHead> second,
    Collector<GraphHead> out) throws Exception {
    boolean isThereAtLeastOneHead = false;
    boolean hasNotBeenInserted = true;
    boolean isBasicInserted = true;
    int isLeft = 0;
    GraphHead toInsert = BASIC;
    // Checking if the left graph has a head
    for (GraphHead left : first) {
      isThereAtLeastOneHead = true;
      toInsert = left;
      tuple2.f0 = left;
      // Checking if the right graph has a head
      for (GraphHead right : second) {
        tuple2.f1 = right;
        if (thetaGraph.apply(tuple2)) {
          GraphHead gh1 = combineHeads.apply(tuple2);
          gh1.setId(gid);
          out.collect(gh1);
          hasNotBeenInserted = false;
          break;
        } else {
          isThereAtLeastOneHead = true;
          hasNotBeenInserted = true;
          isBasicInserted = true;
          toInsert = BASIC;
          break;
        }
      }
    }
    // The fact that the left grpah has no head, does not insure that the right graph has it
    // or not.
    if (! isThereAtLeastOneHead) {
      // Checking if the right graph has the head
      for (GraphHead right : second) {
        isThereAtLeastOneHead = true;
        isBasicInserted = false;
        toInsert = right;
        break;
      }
    }
    // Inserting the element if it has not been inserted yet
    if (isThereAtLeastOneHead && hasNotBeenInserted) {
      if (isBasicInserted) { // Generating a new Id only when strictly required
        toInsert.setId(GradoopId.get());
      }
      out.collect(toInsert);
    }
  }
}
