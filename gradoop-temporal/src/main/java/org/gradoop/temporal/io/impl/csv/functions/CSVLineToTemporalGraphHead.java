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

import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.flink.io.api.metadata.MetaDataSource;
import org.gradoop.flink.io.impl.csv.functions.StringEscaper;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;

import java.util.regex.Matcher;

/**
 * Creates an {@link TemporalGraphHead} from a CSV string. The function uses a {@link MetaData}
 * object to correctly parse the property values.
 *
 * The string needs to be encoded in the following format:
 *
 * {@code graph-id;label;value_1|value_2|...|value_n;(tx-from,tx-to),(val-from,val-to)}
 */
public class CSVLineToTemporalGraphHead extends CSVLineToTemporalElement<TemporalGraphHead> {

  /**
   * Used to instantiate the graph head.
   */
  private final GraphHeadFactory<TemporalGraphHead> graphHeadFactory;

  /**
   * Creates a CSVLineToTemporalGraphHead converter.
   *
   * @param graphHeadFactory the factory that is used to create a graph head object
   */
  public CSVLineToTemporalGraphHead(GraphHeadFactory<TemporalGraphHead> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public TemporalGraphHead map(String csvLine) throws Exception {
    String[] tokens = split(csvLine, 4);
    String label = StringEscaper.unescape(tokens[1]);
    TemporalGraphHead graphHead = graphHeadFactory.initGraphHead(
      GradoopId.fromString(tokens[0]),
      label,
      parseProperties(MetaDataSource.GRAPH_TYPE, label, tokens[2])
    );

    Matcher matcher = TEMPORAL_PATTERN.matcher(tokens[3]);

    validateTemporalData(matcher);
    graphHead.setTransactionTime(parseTransactionTime(matcher));
    graphHead.setValidTime(parseValidTime(matcher));

    return graphHead;
  }
}
