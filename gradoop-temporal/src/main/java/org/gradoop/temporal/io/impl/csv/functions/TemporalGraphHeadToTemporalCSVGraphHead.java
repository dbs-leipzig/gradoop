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

import org.gradoop.flink.io.api.metadata.MetaDataSource;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.functions.StringEscaper;
import org.gradoop.temporal.io.impl.csv.tuples.TemporalCSVGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;

/**
 * Converts an {@link TemporalGraphHead} into a CSV representation.
 */
public class TemporalGraphHeadToTemporalCSVGraphHead
  extends TemporalElementToCSV<TemporalGraphHead, TemporalCSVGraphHead> {

  /**
   * Reduce object instantiations.
   */
  private final TemporalCSVGraphHead csvGraphHead = new TemporalCSVGraphHead();

  @Override
  public TemporalCSVGraphHead map(TemporalGraphHead temporalGraphHead) {
    csvGraphHead.setId(temporalGraphHead.getId().toString());
    csvGraphHead.setLabel(StringEscaper.escape(temporalGraphHead.getLabel(),
      CSVConstants.ESCAPED_CHARACTERS));
    csvGraphHead.setProperties(getPropertyString(temporalGraphHead, MetaDataSource.GRAPH_TYPE));
    csvGraphHead.setTemporalData(getTemporalDataString(temporalGraphHead.getTransactionTime(),
      temporalGraphHead.getValidTime()));
    return csvGraphHead;
  }
}
