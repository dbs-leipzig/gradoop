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
package org.gradoop.flink.model.impl.operators.tpgm.grouping.keys;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.common.model.impl.pojo.temporal.TemporalElement;
import org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute;
import org.gradoop.flink.model.api.tpgm.functions.grouping.GroupingKeyFunction;

import java.util.Objects;

/**
 * A key function extracting a {@link TemporalAttribute} from a {@link TemporalElement}.
 */
public class TimeIntervalKeyFunction<T extends TemporalElement> implements
  GroupingKeyFunction<T, Tuple2<Long, Long>> {

  /**
   * The time interval to extract.
   */
  private final TemporalAttribute timeInterval;

  /**
   * Create a new instance of this key function.
   *
   * @param timeInterval The time interval to extract.
   */
  public TimeIntervalKeyFunction(TemporalAttribute timeInterval) {
    this.timeInterval = Objects.requireNonNull(timeInterval);
  }

  @Override
  public Tuple2<Long, Long> getKey(T element) {
    switch (timeInterval) {
    case VALID_TIME:
      return element.getValidTime();
    case TRANSACTION_TIME:
      return element.getTransactionTime();
    default:
      throw new UnsupportedOperationException(
        "Time interval not supported by this element: " + timeInterval);
    }
  }

  @Override
  public TypeInformation<Tuple2<Long, Long>> getType() {
    return TupleTypeInfo.getBasicTupleTypeInfo(Long.TYPE, Long.TYPE);
  }

  @Override
  public String toString() {
    return timeInterval.toString();
  }
}
