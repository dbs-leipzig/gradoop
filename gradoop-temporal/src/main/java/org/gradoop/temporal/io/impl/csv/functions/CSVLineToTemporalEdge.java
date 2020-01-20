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

import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.flink.io.api.metadata.MetaDataSource;
import org.gradoop.flink.io.impl.csv.functions.StringEscaper;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.regex.Matcher;

/**
 * Creates an {@link TemporalEdge} from a CSV string. The function uses a {@link MetaData}
 * object to correctly parse the property values.
 *
 * The string needs to be encoded in the following format:
 *
 * {@code edge-id;[graph-ids];source-id;target-id;edge-label;value_1|value_2|...|value_n;
 * (tx-from,tx-to),(val-from,val-to)}
 */
public class CSVLineToTemporalEdge extends CSVLineToTemporalElement<TemporalEdge> {

  /**
   * Used to instantiate the edge.
   */
  private final EdgeFactory<TemporalEdge> edgeFactory;

  /**
   * Creates a CSVLineToTemporalEdge converter.
   *
   * @param edgeFactory the factory that is used to create an edge object
   */
  public CSVLineToTemporalEdge(EdgeFactory<TemporalEdge> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  @Override
  public TemporalEdge map(String csvLine) throws Exception {
    String[] tokens = split(csvLine, 7);
    String label = StringEscaper.unescape(tokens[4]);
    TemporalEdge edge = edgeFactory.initEdge(GradoopId.fromString(tokens[0]),
      label,
      GradoopId.fromString(tokens[2]),
      GradoopId.fromString(tokens[3]),
      parseProperties(MetaDataSource.EDGE_TYPE, label, tokens[5]),
      parseGradoopIds(tokens[1]));

    Matcher matcher = TEMPORAL_PATTERN.matcher(tokens[6]);

    validateTemporalData(matcher);
    edge.setTransactionTime(parseTransactionTime(matcher));
    edge.setValidTime(parseValidTime(matcher));

    return edge;
  }
}
