/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.operators.sampling.common.SamplingEvaluationConstants;
import org.gradoop.flink.model.impl.operators.statistics.DegreeCentrality;
import org.gradoop.flink.model.impl.operators.statistics.writer.StatisticWriter;

/**
 * Computes {@link DegreeCentrality} for a given logical graph.
 * It writes the result as a single value into an csv-file:
 * {@value SamplingEvaluationConstants#FILE_DEGREE_CENTRALITY}
 */
public class DegreeCentralityRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Calls the {@link DegreeCentrality} computation for a given logical graph.
   *
   * args[0] - path to input directory
   * args[1] - input format (json, csv, indexed)
   * args[2] - path to output directory
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {

    LogicalGraph logicalGraph = readLogicalGraph(args[0], args[1]);

    DataSet<Tuple1<Double>> resultSet = new DegreeCentrality()
      .execute(logicalGraph)
      .map(new ObjectTo1<>());

    StatisticWriter.writeCSV(resultSet,
      appendSeparator(args[2]) + SamplingEvaluationConstants.FILE_DEGREE_CENTRALITY);

    getExecutionEnvironment().execute("Statistics: Degree centrality");
  }

  @Override
  public String getDescription() {
    return DegreeCentralityRunner.class.getName();
  }
}
