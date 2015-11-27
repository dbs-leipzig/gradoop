package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.counting.Tuple1With1L;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 17.11.15.
 */
public class EqualityByGraphIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  @Override
  public DataSet<Boolean> execute(
    GraphCollection<G, V, E> firstCollection,
    GraphCollection<G, V, E> secondCollection) {

    DataSet<Tuple2<GradoopId, Long>> firstGraphIdsWithCount =
      getIdsWithCount(firstCollection);

    DataSet<Tuple2<GradoopId, Long>> secondGraphIdsWithCount =
      getIdsWithCount(secondCollection);

    DataSet<Tuple1<Long>> distinctFirstIdCount = firstGraphIdsWithCount
      .map(new Tuple1With1L<Tuple2<GradoopId, Long>>())
      .sum(0);

    DataSet<Tuple1<Long>> matchingIdCount = firstGraphIdsWithCount
      .join(secondGraphIdsWithCount)
      .where(0, 1).equalTo(0, 1)
      .with(new Tuple1With1L<Tuple2<GradoopId,Long>>())
      .sum(0);

    return checkCountEqualsCount(distinctFirstIdCount, matchingIdCount);
  }



  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
