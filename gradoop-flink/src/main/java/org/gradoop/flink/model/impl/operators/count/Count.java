/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.count;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.functions.bool.Equals;
import org.gradoop.flink.model.impl.functions.tuple.ValueOf1;
import org.gradoop.flink.model.impl.operators.count.functions.Tuple1With1L;
import org.gradoop.flink.model.impl.operators.count.functions.Tuple2FromTupleWithObjectAnd1L;
import org.gradoop.flink.model.impl.operators.count.functions.Tuple2WithObjectAnd1L;

/**
 * Utility methods to count the number of elements in a dataset without
 * collecting it.
 */
public class Count {

  /**
   * Counts the elements in the given dataset and stores the result in a
   * 1-element dataset.
   *
   * @param dataSet input dataset
   * @param <T>     element type in input dataset
   * @return 1-element dataset with count of input dataset
   */
  public static <T> DataSet<Long> count(DataSet<T> dataSet) {
    return dataSet
      .map(new Tuple1With1L<T>())
      .union(dataSet.getExecutionEnvironment().fromElements(new Tuple1<>(0L)))
      .sum(0)
      .map(new ValueOf1<>());
  }

  /**
   * Counts the elements in the given dataset. If the dataset is empty, a
   * 1-element dataset will be returned containing {@code true}. Otherwise
   * it contains {@code false}.
   *
   * @param dataSet input dataset
   * @param <T>     element type in input dataset
   * @return 1-element dataset containing true iff input dataset is empty
   */
  public static <T> DataSet<Boolean> isEmpty(DataSet<T> dataSet) {
    return Equals.cross(count(dataSet),
      dataSet.getExecutionEnvironment().fromElements(0L));
  }

  /**
   * Groups the input dataset by the contained elements and counts the elements
   * per group. Returns a {@link Tuple2} containing the group element and the
   * corresponding count value.
   *
   * @param dataSet input dataset
   * @param <T>     element type in input dataset
   * @return {@link Tuple2} with group value and group count
   */
  public static <T> DataSet<Tuple2<T, Long>> groupBy(DataSet<T> dataSet) {
    return dataSet
      .map(new Tuple2WithObjectAnd1L<>())
      .groupBy(0)
      .sum(1);
  }

  /**
   * Groups the input dataset by the contained elements and counts the elements
   * per group. Returns a {@link Tuple2} containing the group element and the corresponding
   * count value.
   *
   * @param dataSet input dataset containing tuples
   * @param <T> element type in input dataset
   * @return {@link Tuple2} with group value and group count
   */
  public static <T> DataSet<Tuple2<T, Long>> groupByFromTuple(DataSet<Tuple1<T>> dataSet) {
    return dataSet
      .map(new Tuple2FromTupleWithObjectAnd1L<>())
      .groupBy(0)
      .sum(1);
  }
}
