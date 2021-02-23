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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToElement;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for reading a {@link TemporalElement} from CSV. Handles the {@link MetaData} which is
 * required to parse the property values.
 *
 * @param <E> Temporal element type
 */
abstract class CSVLineToTemporalElement<E extends TemporalElement> extends CSVLineToElement<E> {
  /**
   * Data pattern to parse a temporal string representation of transaction and valid time.
   * The pattern contains four groups that capsule numeric values in format
   * {@code (tx-from,tx-to),(val-from,val-to)} where group 1 contains tx-from value, group 2 tx-to value, etc.
   */
  static final Pattern TEMPORAL_PATTERN =
    Pattern.compile("\\((-?\\d+),(-?\\d+)\\),\\((-?\\d+),(-?\\d+)\\)");

  /**
   * Validates the temporal data matched by the given matcher instance.
   *
   * @param matcher the matcher instance containing the temporal data
   * @throws IOException if the temporal attributes can not be found inside the string
   */
  void validateTemporalData(Matcher matcher) throws IOException {
    if (!matcher.matches() || matcher.groupCount() != 4) {
      throw new IOException("Can not read temporal data from csv line of edge file.");
    }
  }

  /**
   * Parses the transaction time from the temporal data matched by the given matcher instance.
   *
   * @param matcher the matcher instance containing the temporal data
   * @return a tuple containing the transaction time
   */
  Tuple2<Long, Long> parseTransactionTime(Matcher matcher) {
    return new Tuple2<>(Long.valueOf(matcher.group(1)), Long.valueOf(matcher.group(2)));
  }

  /**
   * Parses the valid time from the temporal data matched by the given matcher instance.
   *
   * @param matcher the matcher instance containing the temporal data
   * @return a tuple containing the valid time
   */
  Tuple2<Long, Long> parseValidTime(Matcher matcher) {
    return new Tuple2<>(Long.valueOf(matcher.group(3)), Long.valueOf(matcher.group(4)));
  }
}
