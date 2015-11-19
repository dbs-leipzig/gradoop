package org.gradoop.model.impl.operators.collection.binary.equality;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.counting.ToCountableTuple2;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.counting.OneInTuple1;
import org.gradoop.model.impl.functions.isolation.ElementIdOnly;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 17.11.15.
 */
public class EqualByGraphIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  @Override
  public DataSet<Boolean> execute(
    GraphCollection<V, E, G> firstCollection,
    GraphCollection<V, E, G> secondCollection) {

    DataSet<Tuple2<GradoopId, Long>> firstIdsWithCount =
      getIdsWithCount(firstCollection);

    DataSet<Tuple2<GradoopId, Long>> secondIdsWithCount =
      getIdsWithCount(secondCollection);

    DataSet<Tuple1<Long>> distinctFirstIdCount = firstIdsWithCount
      .map(new OneInTuple1<Tuple2<GradoopId, Long>>())
      .sum(0);

    DataSet<Tuple1<Long>> matchingIdCount = firstIdsWithCount
      .join(secondIdsWithCount)
      .where(0, 1).equalTo(0, 1)
      .with(new OneInTuple1<Tuple2<GradoopId,Long>>())
      .sum(0);

    return matchingIdCount
      .cross(distinctFirstIdCount)
      .with(new Equals<Tuple1<Long>>());
  }

  private AggregateOperator<Tuple2<GradoopId, Long>> getIdsWithCount(
    GraphCollection<V, E, G> graphCollection) {

    return graphCollection
      .getGraphHeads()
      .map(new ElementIdOnly<G>())
      .map(new ToCountableTuple2<GradoopId>())
      .groupBy(0)
      .sum(1);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
