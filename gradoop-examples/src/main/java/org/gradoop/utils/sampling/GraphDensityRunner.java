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
package org.gradoop.utils.sampling;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.operators.sampling.evaluation.GraphDensity;
import org.gradoop.flink.model.impl.operators.sampling.evaluation.SamplingEvaluationConstants;
import org.gradoop.flink.model.impl.operators.statistics.writer.StatisticWriter;

/**
 * Calls the graph density computation for a logical graph and a graph sampled from it.
 * Writes both results to files in the given directory.
 */
public class GraphDensityRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Calls the graph density computation for both graphs.
   *
   * @param graph Original logical graph
   * @param sample Sampled logical graph
   * @param outputDir Directory for evaluation results
   */
  public static void main(LogicalGraph graph, LogicalGraph sample, String outputDir)
    throws Exception {

    DataSet<Double> densityOriginal = graph.callForGraph(new GraphDensity()).getGraphHead()
      .map(new MapFunction<GraphHead, Double>() {
        @Override
        public Double map(GraphHead graphHead) throws Exception {
          return graphHead.getPropertyValue(SamplingEvaluationConstants.PROPERTY_KEY_DENSITY)
            .getDouble();
        }
      });

    DataSet<Double> densitySampled = sample.callForGraph(new GraphDensity()).getGraphHead()
      .map(new MapFunction<GraphHead, Double>() {
        @Override
        public Double map(GraphHead graphHead) throws Exception {
          return graphHead.getPropertyValue(SamplingEvaluationConstants.PROPERTY_KEY_DENSITY)
            .getDouble();
        }
      });

    StatisticWriter.writeCSV(densityOriginal.map(new ObjectTo1<>()),
      appendSeparator(outputDir) + SamplingEvaluationConstants.FILE_DENSITY_ORIGINAL);
    StatisticWriter.writeCSV(densitySampled.map(new ObjectTo1<>()),
      appendSeparator(outputDir) + SamplingEvaluationConstants.FILE_DENSITY_SAMPLED);

    getExecutionEnvironment().execute("Sampling Evaluation: Graph density");
  }

  @Override
  public String getDescription() {
    return GraphDensityRunner.class.getName();
  }
}
