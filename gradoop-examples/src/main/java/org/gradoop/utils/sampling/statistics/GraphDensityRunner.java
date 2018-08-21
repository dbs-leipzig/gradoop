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
import org.gradoop.flink.model.impl.operators.sampling.statistics.GraphDensity;
import org.gradoop.flink.model.impl.operators.sampling.statistics.SamplingEvaluationConstants;
import org.gradoop.flink.model.impl.operators.statistics.writer.StatisticWriter;
import org.gradoop.utils.sampling.statistics.functions.GetPropertyValueDouble;

/**
 * Calls the graph density computation for a logical graph and a graph sampled from it.
 * Writes both results to files in the output directory.
 */
public class GraphDensityRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Calls the graph density computation for both graphs.
   *
   * args[0] - path to original graph
   * args[1] - format of original graph (csv, json, indexed)
   * args[2] - path to sampled graph
   * args[3] - format of sampled graph (csv, json, indexed)
   * args[4] - output path
   *
   * @param args command line arguments
   * @throws Exception in case of read/write failure
   */
  public static void main(String[] args) throws Exception {

    LogicalGraph origin = readLogicalGraph(args[0], args[1]);
    LogicalGraph sampled = readLogicalGraph(args[2], args[3]);

    DataSet<Double> densityOriginal = origin.callForGraph(new GraphDensity()).getGraphHead()
      .map(new GetPropertyValueDouble(SamplingEvaluationConstants.PROPERTY_KEY_DENSITY));

    DataSet<Double> densitySampled = sampled.callForGraph(new GraphDensity()).getGraphHead()
      .map(new GetPropertyValueDouble(SamplingEvaluationConstants.PROPERTY_KEY_DENSITY));

    StatisticWriter.writeCSV(densityOriginal.map(new ObjectTo1<>()),
      appendSeparator(args[4]) + SamplingEvaluationConstants.FILE_DENSITY_ORIGINAL);
    StatisticWriter.writeCSV(densitySampled.map(new ObjectTo1<>()),
      appendSeparator(args[4]) + SamplingEvaluationConstants.FILE_DENSITY_SAMPLED);

    getExecutionEnvironment().execute("Sampling Evaluation: Graph density");
  }

  @Override
  public String getDescription() {
    return GraphDensityRunner.class.getName();
  }
}
