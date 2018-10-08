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
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.tuples.CSVGraphHead;

/**
 * Converts an {@link org.gradoop.common.model.impl.pojo.GraphHead} into a CSV representation.
 *
 * Forwarded fields:
 *
 * label
 */
@FunctionAnnotation.ForwardedFields("label->f1")
public class GraphHeadToCSVGraphHead extends ElementToCSV<GraphHead, CSVGraphHead> {
  /**
   * Reduce object instantiations
   */
  private final CSVGraphHead csvGraphHead = new CSVGraphHead();

  @Override
  public CSVGraphHead map(GraphHead graphHead) throws Exception {
    csvGraphHead.setId(graphHead.getId().toString());
    csvGraphHead.setLabel(StringEscaper.escape(graphHead.getLabel(),
      CSVConstants.ESCAPED_CHARACTERS));
    csvGraphHead.setProperties(getPropertyString(graphHead, CSVConstants.GRAPH_TYPE));
    return csvGraphHead;
  }
}
