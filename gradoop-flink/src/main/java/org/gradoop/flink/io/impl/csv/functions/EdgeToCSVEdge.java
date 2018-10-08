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

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.tuples.CSVEdge;

/**
 * Converts an {@link Edge} into a CSV representation.
 *
 * Forwarded fields:
 *
 * label
 */
@FunctionAnnotation.ForwardedFields("label->f4")
public class EdgeToCSVEdge extends ElementToCSV<Edge, CSVEdge> {
  /**
   * Reduce object instantiations
   */
  private final CSVEdge csvEdge = new CSVEdge();

  @Override
  public CSVEdge map(Edge edge) throws Exception {
    csvEdge.setId(edge.getId().toString());
    csvEdge.setGradoopIds(collectionToCsvString(edge.getGraphIds()));
    csvEdge.setSourceId(edge.getSourceId().toString());
    csvEdge.setTargetId(edge.getTargetId().toString());
    csvEdge.setLabel(StringEscaper.escape(edge.getLabel(), CSVConstants.ESCAPED_CHARACTERS));
    csvEdge.setProperties(getPropertyString(edge, CSVConstants.EDGE_TYPE));
    return csvEdge;
  }
}
