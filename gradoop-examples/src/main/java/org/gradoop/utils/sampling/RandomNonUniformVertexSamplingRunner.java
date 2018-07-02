/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.utils.sampling;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.RandomNonUniformVertexSampling;

/**
 * Runs {@link RandomNonUniformVertexSampling} for a given
 * graph and writes the sampled output.
 */
public class RandomNonUniformVertexSamplingRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Runs the {@link RandomNonUniformVertexSampling} operator
   * on the specified graph and writes the result to the specified output.
   *
   * args[0] - path to input graph
   * args[1] - format of input graph
   * args[2] - vertex probability
   * args[3] - path to output
   *
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);
    LogicalGraph sample = new RandomNonUniformVertexSampling(Float.parseFloat(args[2])).execute(graph);
    new CSVDataSink(args[3], args[0] + "/metadata.csv", graph.getConfig()).write(sample);
    getExecutionEnvironment().execute("Random Non Uniform Vertex Sampling (" + args[2] + ")");
  }

  @Override
  public String getDescription() {
    return RandomNonUniformVertexSamplingRunner.class.getName();
  }
}
