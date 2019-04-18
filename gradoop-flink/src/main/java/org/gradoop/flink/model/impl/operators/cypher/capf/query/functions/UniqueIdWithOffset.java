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
package org.gradoop.flink.model.impl.operators.cypher.capf.query.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * Zip each element in the dataset with a unique Long id. Its possible to set an offset via a
 * broadcast set.
 *
 * @param <E> the type of elements in the dataset
 */

public class UniqueIdWithOffset<E> extends RichMapFunction<E, Tuple2<Long, E>> {

  /**
   * The parallelism this MapFunction is run at.
   */
  private Long parallelism;

  /**
   * A counter to make sure each id is only given once.
   */
  private Long idCounter;

  /**
   * Reduce object instantiations
   */
  private Tuple2<Long, E> returnTuple = new Tuple2<>();

  @Override
  public void open(Configuration parameters) {

    idCounter = 0L;

    RuntimeContext ctx = getRuntimeContext();
    // if an offset has been set, add it to the idCounter
    if (ctx.hasBroadcastVariable("offset")) {
      List<Long> offset = ctx.getBroadcastVariable("offset");
      if (offset.size() > 0) {
        idCounter += offset.get(0);
      }
    }
    parallelism = (long) ctx.getNumberOfParallelSubtasks();
    idCounter += (long) ctx.getIndexOfThisSubtask();
  }

  @Override
  public Tuple2<Long, E> map(E e) throws Exception {
    returnTuple.f0 = idCounter;
    returnTuple.f1 = e;
    idCounter += parallelism;
    return returnTuple;
  }
}
