/**
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
package org.gradoop.utils.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsReader;
import org.gradoop.flink.model.impl.operators.statistics.DistinctVertexProperties;

/**
 * Computes {@link DistinctVertexProperties} for a given logical graph.
 */
public class DistinctVertexPropertiesRunner extends AbstractRunner implements ProgramDescription {

  /**
   * args[0] - path to input directory
   * args[1] - input format (json, csv)
   * args[2] - path to output directory
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    new DistinctVertexProperties()
      .execute(readLogicalGraph(args[0], args[1]))
      .writeAsCsv(
        appendSeparator(args[2]) + GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    getExecutionEnvironment().execute("Statistics: Distinct vertex properties");
  }

  @Override
  public String getDescription() {
    return DistinctVertexPropertiesRunner.class.getName();
  }
}
