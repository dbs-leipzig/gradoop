package org.gradoop.model.impl.operators.collection.binary.equality.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.counting.ToCountableTuple2;
import org.gradoop.model.impl.functions.isolation.ElementIdOnly;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 19.11.15.
 */
public class EqualityHelper {

  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  AggregateOperator<Tuple2<GradoopId, Long>> getIdsWithCount(
    GraphCollection<V, E, G> graphCollection
  ) {

    return graphCollection
      .getGraphHeads()
      .map(new ElementIdOnly<G>())
      .map(new ToCountableTuple2<GradoopId>())
      .groupBy(0)
      .sum(1);
  }

  public static CrossOperator<Tuple1<Long>, Tuple1<Long>, Boolean>
  checkCountEqualsCount(
    DataSet<Tuple1<Long>> firstCount, DataSet<Tuple1<Long>> matchingIdCount) {
    return matchingIdCount.cross(firstCount).with(new Equals<Tuple1<Long>>());
  }
}
