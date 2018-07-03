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
package org.gradoop.utils.sampling;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.RandomEdgeSampling;

/**
 * Runs {@link org.gradoop.flink.model.impl.operators.sampling.RandomEdgeSampling} for a given
 * graph and writes the sampled output.
 */
public class RandomEdgeSamplingRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Runs the {@link org.gradoop.flink.model.impl.operators.sampling.RandomEdgeSampling} operator
   * on the specified graph and writes the result to the specified output.
   *
   * args[0] - path to input graph
   * args[1] - format of input graph
   * args[2] - edge probability
   * args[3] - path to output
   *
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);
    LogicalGraph sample = graph.callForGraph(new RandomEdgeSampling(Float.parseFloat(args[2])));
    writeLogicalGraph(sample,args[0]);
    getExecutionEnvironment().execute("Random Edge Sampling (" + args[2] + ")");
  }

  @Override
  public String getDescription() {
    return RandomEdgeSamplingRunner.class.getName();
  }
}
