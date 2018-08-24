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
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexNeighborhoodSampling;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;

/**
 * Runs {@link RandomVertexNeighborhoodSampling} for a given
 * graph and writes the sampled output.
 */
public class RandomVertexNeighborhoodSamplingRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Runs the {@link RandomVertexNeighborhoodSampling} operator
   * on the specified graph and writes the result to the specified output.
   * <p>
   * args[0] - path to input graph
   * args[1] - format of input graph (csv, json, indexed)
   * args[2] - path to output graph
   * args[3] - format of output graph (csv, json, indexed)
   * args[4] - sampling threshold
   * args[5] - type of neighborhood (IN, OUT, BOTH)
   *
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);

    LogicalGraph sample = graph.callForGraph(
      new RandomVertexNeighborhoodSampling(
        Float.parseFloat(args[4]), Neighborhood.valueOf(args[5])));

    writeLogicalGraph(sample, args[2], args[3]);
  }

  @Override
  public String getDescription() {
    return RandomVertexNeighborhoodSamplingRunner.class.getName();
  }
}
