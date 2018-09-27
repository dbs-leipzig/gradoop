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
package org.gradoop.flink.io.impl.deprecated.logicalgraphcsv;

import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.csv.CSVConstants;

/**
 * Creates an {@link Edge} from a CSV string. The function uses a
 * {@link MetaData} object to correctly parse the property values.
 *
 * The string needs to be encoded in the following format:
 *
 * {@code edge-id;source-id;target-id;edge-label;value_1|value_2|...|value_n}
 */
public class CSVLineToEdge extends CSVLineToElement<Edge> {
  /**
   * Used to instantiate the edge.
   */
  private final EPGMEdgeFactory<Edge> edgeFactory;

  /**
   * Constructor.
   *
   * @param epgmEdgeFactory EPGM edge factory
   */
  public CSVLineToEdge(EPGMEdgeFactory<Edge> epgmEdgeFactory) {
    this.edgeFactory = epgmEdgeFactory;
  }

  @Override
  public Edge map(String csvLine) {
    String[] tokens = split(csvLine, 5);
    return edgeFactory.initEdge(GradoopId.fromString(tokens[0]),
      tokens[3],
      GradoopId.fromString(tokens[1]),
      GradoopId.fromString(tokens[2]),
      parseProperties(CSVConstants.EDGE_TYPE, tokens[3], tokens[4]));
  }
}
