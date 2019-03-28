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
package org.gradoop.utils.sampling;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.RandomWalkSampling;

/**
 * Runs {@link RandomWalkSampling} for a given
 * graph and writes the sampled output.
 */
public class RandomWalkSamplingRunner extends AbstractRunner implements ProgramDescription {
  /**
   * Runs the {@link RandomWalkSampling} operator
   * on the specified graph and writes the result to the specified output.
   *
   * args[0] - path to input graph
   * args[1] - format of input graph (csv, json, indexed)
   * args[2] - sample size
   * args[3] - number of start vertices
   * args[4] - jump probability
   * args[5] - max iteration
   * args[6] - path to output graph
   * args[7] - format of output graph (csv, json, indexed)
   *
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {

    LogicalGraph graph = readLogicalGraph(args[0], args[1]);

    LogicalGraph sampledGraph = graph.callForGraph(new RandomWalkSampling(
      Double.parseDouble(args[2]), Integer.parseInt(args[3]),
      Double.parseDouble(args[4]), Integer.parseInt(args[5])));

    writeLogicalGraph(sampledGraph, args[6], args[7]);
  }

  @Override
  public String getDescription() {
    return RandomWalkSamplingRunner.class.getName();
  }
}
