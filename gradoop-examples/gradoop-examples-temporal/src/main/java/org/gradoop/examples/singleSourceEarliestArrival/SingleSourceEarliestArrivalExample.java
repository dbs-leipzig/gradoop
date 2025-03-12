/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.examples.singleSourceEarliestArrival;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraph;

/**
 * Single Source Earliest Arrival Example
 */
public class SingleSourceEarliestArrivalExample {

  /**
   * Method to run Single Source Earliest Arrival Example
   * @param args Input arguments
   * @throws Exception Exception
   */
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    TemporalGraph in = TemporalSSEAExampleGraph.getTemporalGraph(GradoopFlinkConfig.createConfig(env));

    TemporalGraph out = in.singleSourceEarliestArrival(
            TemporalSSEAExampleGraph.getSrcVertexId(), 100, "SSEA",
            true, 0L, true);

    System.out.println("Output");

    out.print();





  }
}
