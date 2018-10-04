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
package org.gradoop.utils.converter;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 * Converts a given graph into another gradoop format.
 */
public class GradoopFormatConverter extends AbstractRunner implements ProgramDescription {

  /**
   * Converts a graph from one gradoop format to another.
   *
   * args[0] - path to input graph
   * args[1] - format of input graph (lgcsv, lgindexed, csv, json, indexed)
   * args[2] - path to output graph
   * args[3] - format of output graph (csv, json, indexed)
   *
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);
    writeLogicalGraph(graph, args[2], args[3]);
  }

  @Override
  public String getDescription() {
    return GradoopFormatConverter.class.getName();
  }
}
