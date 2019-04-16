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
import org.gradoop.flink.model.impl.operators.sampling.RandomLimitedDegreeVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;

/**
 * Runs {@link org.gradoop.flink.model.impl.operators.sampling.RandomLimitedDegreeVertexSampling}
 * for a given graph and writes the sampled output.
 */
public class RandomLimitedDegreeVertexSamplingRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Runs the
   * {@link org.gradoop.flink.model.impl.operators.sampling.RandomLimitedDegreeVertexSampling}
   * operator on the specified graph and writes the result to the specified output.
   *
   * args[0] - path to input graph
   * args[1] - format of input graph (csv, json, indexed)
   * args[2] - path to output
   * args[3] - format of output graph (csv, json, indexed)
   * args[4] - sampling threshold
   * args[5] - vertex degree threshold
   * args[6] - vertex degree type (IN, OUT, BOTH)
   *
   * @param args arguments
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);

    LogicalGraph sample = graph.callForGraph(
      new RandomLimitedDegreeVertexSampling(
        Float.parseFloat(args[4]), Long.parseLong(args[5]), VertexDegree.valueOf(args[6])));

    writeLogicalGraph(sample, args[2], args[3]);
  }

  @Override
  public String getDescription() {
    return RandomLimitedDegreeVertexSampling.class.getName();
  }
}
