package org.gradoop.flink.model.impl.operators.join.blocks;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.functions.Oplus;

import java.io.Serializable;

/**
 *
 * Defines the way to define a new graph head from two other ones
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class CoJoinGraphHeads  implements CoGroupFunction<GraphHead, GraphHead, GraphHead>,
  Serializable {
  private final Function<Tuple2<GraphHead, GraphHead>, Boolean> thetaGraph;
  private final Oplus<GraphHead> combineHeads;
  private GradoopId gid;

  private static final GraphHead basic = new GraphHead();

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
    boolean hasToBeInserted = false;
    boolean hasNotBeenInserted = true;
    boolean isBasicInserted = true;
    int isLeft = 0;
    GraphHead toInsert = basic;
    for (GraphHead left : first) {
      hasToBeInserted = true;
      if (isLeft==0) {
        isLeft = 1;
        toInsert = left;
      }
      for (GraphHead right : second) {
        if (thetaGraph.apply(new Tuple2<>(left,right))) {
          if (isLeft==1) isLeft = 3;
          GraphHead gh1 = combineHeads.apply(new Tuple2<GraphHead,GraphHead>(left,right));
          gh1.setId(gid);
          out.collect(gh1);
          hasNotBeenInserted = false;
        } else {
          hasToBeInserted = true;
          hasNotBeenInserted = true;
          isBasicInserted = true;
          toInsert = basic;
        }
      }
    }
    if (! hasToBeInserted) {
      for (GraphHead right : second) {
        hasToBeInserted = true;
        isBasicInserted = false;
        isLeft = 2;
        toInsert = right;
        break;
      }
    }
    if (hasToBeInserted && hasNotBeenInserted) {
      if (isBasicInserted) // Generating a new Id only when strictly required
        toInsert.setId(GradoopId.get());
      out.collect(toInsert);
    }
  }
}
