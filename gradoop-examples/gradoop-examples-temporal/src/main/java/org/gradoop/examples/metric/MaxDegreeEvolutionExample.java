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
package org.gradoop.examples.metric;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.MaxDegreeEvolution;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;

/**
 * A self contained example on how to use the max degree evolution operator on a graph loaded via path name.
 */

public class MaxDegreeEvolutionExample {


  /**
   *
   * Runs the example by creating an execution environment, loading a temporal graph from a csv-file
   * and then uses the MaxDegreeEvolution operator on the graph
   *
   *
   * @param args Command line input: path to the csv file for the graph
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = getExecutionEnvironment();

    TemporalGraph graph = new TemporalCSVDataSource(args[0], TemporalGradoopConfig.createConfig(env))
            .getTemporalGraph();



    final DataSet<Tuple3<Long, Long, Float>> resultDataSet = graph
            .callForValue(new MaxDegreeEvolution(VertexDegree.IN, TimeDimension.VALID_TIME));


    resultDataSet.collect();
    //resultDataSet.print();
  }
}
