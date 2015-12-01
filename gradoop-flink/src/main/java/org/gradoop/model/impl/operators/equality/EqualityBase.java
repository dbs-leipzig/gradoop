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
import org.gradoop.model.impl.functions.bool.And;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.counting.Tuple2WithObjectAnd1L;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Superclass of all equality operators.
 */
public abstract class EqualityBase {

  /**
   * collection => (graphId, count)
   *
   * @param graphCollection input collection
   * @param <G> graph head type
   * @param <V> vertex type
   * @param <E> edge type
   * @return graph id count
   */
  public
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  AggregateOperator<Tuple2<GradoopId, Long>> getIdsWithCount(
    GraphCollection<G, V, E> graphCollection
  ) {

    return graphCollection
      .getGraphHeads()
      .map(new Id<G>())
      .map(new Tuple2WithObjectAnd1L<GradoopId>())
      .groupBy(0)
      .sum(1);
  }

  /**
   * (count1) == (count2)
   *
   * @param firstCount first count
   * @param secondCount second count
   * @return true, if equal
   */
  public DataSet<Boolean>
  checkCountEqualsCount(
    DataSet<Tuple1<Long>> firstCount, DataSet<Tuple1<Long>> secondCount) {

    DataSet<Boolean> resultSet =
      secondCount.cross(firstCount).with(new Equals<Tuple1<Long>>());

    resultSet = ensureBooleanSetIsNotEmpty(resultSet);

    return resultSet;
  }

  /**
   * Ensures that a boolean dataset is not empty.
   *
   * @param resultSet empty or non-empty boolean dataset
   * @return non-empty boolean dataset
   */
  protected DataSet<Boolean> ensureBooleanSetIsNotEmpty(
    DataSet<Boolean> resultSet) {

    resultSet = resultSet
      .union(
        resultSet
          .getExecutionEnvironment()
          .fromCollection(Lists.newArrayList(true))).reduce(new And());
    return resultSet;
  }
}
