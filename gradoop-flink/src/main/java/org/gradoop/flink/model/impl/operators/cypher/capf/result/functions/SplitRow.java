/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.cypher.capf.result.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Function that splits a row into multiple tuples. For every field i between start and end
 * a new tuple is created, such that the first field is equal to field i and the second field is
 * equal to the last field of the row. The i-th field is expected to be a Long, the last field a
 * {@link GradoopId}.
 */
public class SplitRow implements FlatMapFunction<Row, Tuple2<Long, GradoopId>> {

  /**
   * The start index of long fields that will be used to create new tuples.
   */
  private int start;

  /**
   * The end index of long fields that will be used to create new tuples.
   */
  private int end;

  /**
   * Reduce object instantiations.
   */
  private Tuple2<Long, GradoopId> reuseTuple = new Tuple2<>();

  /**
   * Constructor.
   *
   * @param start start index of long fields used to create new tuples
   * @param end   end index of long fields used to create new tuples
   */
  public SplitRow(int start, int end) {
    this.start = start;
    this.end = end;
  }

  @Override
  public void flatMap(Row row, Collector<Tuple2<Long, GradoopId>> collector) throws Exception {
    for (int i = start; i < end; i++) {
      reuseTuple.f0 = (Long) row.getField(i);
      reuseTuple.f1 = (GradoopId) row.getField(row.getArity() - 1);
      collector.collect(reuseTuple);
    }
  }
}
