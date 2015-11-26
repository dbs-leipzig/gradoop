package org.gradoop.model.impl.operators.equality;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.bool.Or;
import org.gradoop.model.impl.functions.counting.Tuple2WithObjectAnd1L;
import org.gradoop.model.impl.functions.epgm.ElementId;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 19.11.15.
 */
public abstract class EqualityBase {

  public
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  AggregateOperator<Tuple2<GradoopId, Long>> getIdsWithCount(
    GraphCollection<G, V, E> graphCollection
  ) {

    return graphCollection
      .getGraphHeads()
      .map(new ElementId<G>())
      .map(new Tuple2WithObjectAnd1L<GradoopId>())
      .groupBy(0)
      .sum(1);
  }

  public DataSet<Boolean>
  checkCountEqualsCount(
    DataSet<Tuple1<Long>> firstCount, DataSet<Tuple1<Long>> matchingIdCount) {

    DataSet<Boolean> resultSet =
      matchingIdCount.cross(firstCount).with(new Equals<Tuple1<Long>>());

    resultSet = ensureBooleanSetIsNotEmpty(resultSet);

    return resultSet;
  }

  protected DataSet<Boolean> ensureBooleanSetIsNotEmpty(
    DataSet<Boolean> resultSet) {
    resultSet = resultSet
      .union(
        resultSet
          .getExecutionEnvironment()
          .fromCollection(Lists.newArrayList(false))
      ).reduce(new Or());
    return resultSet;
  }
}
