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
package org.gradoop.flink.io.impl.csv.functions;

import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;

/**
 * Creates a {@link GraphHead} from a CSV string. The function uses a
 * {@link MetaData} object to correctly parse the property values.
 *
 * The string needs to be encoded in the following format:
 *
 * {@code graph-id;graph-label;value_1|value_2|...|value_n}
 */
public class CSVLineToGraphHead extends CSVLineToElement<GraphHead> {
  /**
   * Used to create the graph head.
   */
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;

  /**
   * Creates a CSVLineToGraphHead converter
   *
   * @param graphHeadFactory The factory class that is used to create the graph heads.
   */
  public CSVLineToGraphHead(EPGMGraphHeadFactory<GraphHead> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public GraphHead map(String csvLine) throws Exception {
    String[] tokens = split(csvLine, 3);
    String label = StringEscaper.unescape(tokens[1]);
    return graphHeadFactory.initGraphHead(
      GradoopId.fromString(tokens[0]),
      label,
      parseProperties(CSVConstants.GRAPH_TYPE, label, tokens[2])
    );
  }
}
