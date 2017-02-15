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
   * Function describing if the two graph heads match or not.
   */
  private final Function<Tuple2<GraphHead, GraphHead>, Boolean> thetaGraph;
  /**
   * Function for combining the matching graph heads together
   */
  private final Oplus<GraphHead> combineHeads;
  /**
   * Basic graph head that could be returned when an empty graph should be returned (no matching
   * graphs)
   */
  private static final GraphHead basic = new GraphHead();
  /**
   * Graph id to which the GraphHead belongs to
   */
  private GradoopId gid;

  public CoJoinGraphHeads(Function<Tuple2<GraphHead, GraphHead>, Boolean> thetaGraph,
    Oplus<GraphHead> combineHeads, GradoopId gid) {
    this.thetaGraph = thetaGraph;
    this.combineHeads = combineHeads;
    this.gid = gid;
  }

  public CoJoinGraphHeads(Function<Tuple2<GraphHead, GraphHead>, Boolean> thetaGraph,
    Oplus<GraphHead> combineHeads) {
    this.thetaGraph = thetaGraph;
    this.combineHeads = combineHeads;
    this.gid = null;
  }

  public CoJoinGraphHeads setGraphId(GradoopId id) {
    this.gid = id;
    return this;
  }

  @Override
  public void coGroup(Iterable<GraphHead> first, Iterable<GraphHead> second,
    Collector<GraphHead> out) throws Exception {
    boolean joinConditionIsSatisfied = false;
    boolean hasNotBeenInserted = true;
    boolean isBasicInserted = true;
    int isLeft = 0;
    GraphHead toInsert = basic;
    for (GraphHead left : first) {
      joinConditionIsSatisfied = true;
      if (isLeft==0) {
        isLeft = 1;
        toInsert = left;
      }
      for (GraphHead right : second) {
        if (thetaGraph.apply(new Tuple2<>(left,right))) {
          if (isLeft==1) isLeft = 3;
          GraphHead gh1 = combineHeads.apply(new Tuple2<>(left, right));
          gh1.setId(gid);
          out.collect(gh1);
          hasNotBeenInserted = false;
        } else {
          joinConditionIsSatisfied = true;
          hasNotBeenInserted = true;
          isBasicInserted = true;
          toInsert = basic;
        }
      }
    }
    if (! joinConditionIsSatisfied) {
      for (GraphHead right : second) {
        joinConditionIsSatisfied = true;
        isBasicInserted = false;
        toInsert = right;
        break;
      }
    }
    if (joinConditionIsSatisfied && hasNotBeenInserted) {
      if (isBasicInserted) // Generating a new Id only when strictly required
        toInsert.setId(GradoopId.get());
      out.collect(toInsert);
    }
  }
}
