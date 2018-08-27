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
package org.gradoop.utils.sampling.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.operators.sampling.statistics.AverageOutgoingDegree;
import org.gradoop.flink.model.impl.operators.sampling.statistics.SamplingEvaluationConstants;
import org.gradoop.flink.model.impl.operators.statistics.writer.StatisticWriter;

/**
 * Calls the average outgoing degree computation for a logical graph. Writes the result to a
 * csv-file named {@value SamplingEvaluationConstants#FILE_AVERAGE_OUTGOING_DEGREE}
 * in the output directory, containing a single line with the average outgoing degree value, e.g.:
 * <pre>
 * BOF
 * 4
 * EOF
 * </pre>
 */
public class AverageOutgoingDegreeRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Calls the average outgoing degree computation for the graph.
   *
   * <pre>
   * args[0] - path to graph
   * args[1] - format of graph (csv, json, indexed)
   * args[2] - output path
   * </pre>
   *
   * @param args command line arguments
   * @throws Exception in case of read/write failure
   */
  public static void main(String[] args) throws Exception {

    LogicalGraph graph = readLogicalGraph(args[0], args[1]);

    DataSet<Long> averageOutgoingDegree = graph.callForGraph(new AverageOutgoingDegree())
      .getGraphHead()
      .map(gh -> gh.getPropertyValue(
        SamplingEvaluationConstants.PROPERTY_KEY_AVERAGE_OUTGOING_DEGREE).getLong());

    StatisticWriter.writeCSV(averageOutgoingDegree.map(new ObjectTo1<>()),
      appendSeparator(args[2]) + SamplingEvaluationConstants.FILE_AVERAGE_OUTGOING_DEGREE);

    getExecutionEnvironment().execute("Sampling Statistics: Average outgoing degree");
  }

  @Override
  public String getDescription() {
    return AverageOutgoingDegreeRunner.class.getName();
  }
}
