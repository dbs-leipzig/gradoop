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
import org.gradoop.flink.model.impl.operators.sampling.PageRankSampling;

/**
 * Runs {@link PageRankSampling} for a given graph and writes the sampled output.
 */
public class PageRankSamplingRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Runs the {@link PageRankSampling} operator on the specified graph and writes the result to
   * the specified output.
   *
   * <pre>
   * args[0] - path to input graph
   * args[1] - format of input graph (csv, json, indexed)
   * args[2] - path to output graph
   * args[3] - format of output graph (csv, json, indexed)
   * args[4] - dampening factor (between 0.0 and 1.0)
   * args[5] - number of iterations
   * args[6] - sampling threshold for PageRank-score (between 0.0 and 1.0)
   * args[7] - sample vertices with score greater than threshold (true, false)
   * args[8] - sample all vertices if all got same score (true, false)
   * </pre>
   *
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);

    LogicalGraph sample = graph.sample(new PageRankSampling(
      Double.parseDouble(args[4]), Integer.parseInt(args[5]), Double.parseDouble(args[6]),
      Boolean.parseBoolean(args[7]), Boolean.parseBoolean(args[8])));

    writeLogicalGraph(sample, args[2], args[3]);
  }

  @Override
  public String getDescription() {
    return PageRankSamplingRunner.class.getName();
  }
}
