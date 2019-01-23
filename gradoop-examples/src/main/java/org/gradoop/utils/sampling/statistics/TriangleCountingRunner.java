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
package org.gradoop.utils.sampling.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.gelly.trianglecounting.GellyTriangleCounting;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.operators.sampling.statistics.SamplingEvaluationConstants;
import org.gradoop.flink.model.impl.operators.statistics.writer.StatisticWriter;

/**
 * Calls the computation of the triangle count (closed triplets) for a logical graph.
 * Uses the Gradoop-Wrapper {@link GellyTriangleCounting} of Flinks TriangleEnumerator-algorithm.
 * Writes the value to a csv-file named {@value SamplingEvaluationConstants#FILE_TRIANGLE_COUNT}
 * in the output directory, e.g.:
 * <pre>
 * BOF
 * 8
 * EOF
 * </pre>
 */
public class TriangleCountingRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Calls the computation of the triangle count (closed triplets) for the graph.
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

    DataSet<Long> triangleCount = new GellyTriangleCounting().execute(graph).getGraphHead()
      .map(gh -> gh.getPropertyValue(GellyTriangleCounting.PROPERTY_KEY_TRIANGLES).getLong());

    StatisticWriter.writeCSV(triangleCount.map(new ObjectTo1<>()),
      appendSeparator(args[2]) + SamplingEvaluationConstants.FILE_TRIANGLE_COUNT);

    getExecutionEnvironment().execute("Sampling Statistics: Triangle count");
  }

  @Override
  public String getDescription() {
    return TriangleCountingRunner.class.getName();
  }
}
