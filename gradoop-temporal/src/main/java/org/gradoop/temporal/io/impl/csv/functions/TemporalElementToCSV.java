/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.io.impl.csv.functions;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.csv.functions.ElementToCSV;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

/**
 * Base class to convert an element into a CSV representation.
 *
 * @param <E> The element type.
 * @param <T> The output tuple type.
 */
abstract class TemporalElementToCSV<E extends TemporalElement, T extends Tuple> extends ElementToCSV<E, T> {

  /**
   * Returns a CSV string representation of the temporal attributes in format:
   *
   * {@code (tx-from,tx-to),(val-from,val-to)}
   *
   * @param transactionTime the transaction time
   * @param validTime the valid time
   * @return CSV string representation
   */
  String getTemporalDataString(Tuple2<Long, Long> transactionTime, Tuple2<Long, Long> validTime) {
    return String.format("(%d,%d),(%d,%d)",
      transactionTime.f0,
      transactionTime.f1,
      validTime.f0,
      validTime.f1);
  }
}
