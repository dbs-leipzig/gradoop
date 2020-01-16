/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.examples.aggregation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.common.TemporalCitiBikeGraph;
import org.gradoop.examples.common.functions.TransformLongPropertiesToDateTime;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.AverageVertexDuration;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MaxVertexTime;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MinVertexTime;

/**
 * A self contained example on how to use the aggregation operator of Gradoop's TPGM model designed to analyse
 * temporal graphs.
 */
public class TemporalAggregationExample {

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview of the usage of the {@code aggregate()} method with pre-defined
   * aggregate functions suitable for temporal analysis.
   *
   * Documentation for all available aggregation functions as well as a detailed description of the
   * aggregate method can be found in the projects wiki.
   *
   * Using the bike graph in the {@link TemporalCitiBikeGraph} class, the program will:
   * <ol>
   *   <li>calculate the earliest start of a trip</li>
   *   <li>calculate the earliest end of a trip</li>
   *   <li>calculate the last start of a trip</li>
   *   <li>calculate the last end of a trip</li>
   *   <li>convert the epoch timestamps to a readable format</li>
   *   <li>calculate the average duration of a trip</li>
   * </ol>
   * The result is stored in the graph head properties. The graph head will be printed to console.
   *
   * @param args Command line arguments (unused).
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Temporal-Graph-Support#aggregation">
   * Gradoop Wiki</a>
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    // init execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // Load the bike graph
    TemporalGraph bikeGraph = TemporalCitiBikeGraph.getTemporalGraph(GradoopFlinkConfig.createConfig(env));

    bikeGraph
      // apply four aggregate functions to get the earliest and latest start and end of a duration
      .aggregate(
        new MinVertexTime("earliestStart", TimeDimension.VALID_TIME, TimeDimension.Field.FROM),
        new MinVertexTime("earliestEnd", TimeDimension.VALID_TIME, TimeDimension.Field.TO),
        new MaxVertexTime("lastStart", TimeDimension.VALID_TIME, TimeDimension.Field.FROM),
        new MaxVertexTime("lastEnd", TimeDimension.VALID_TIME, TimeDimension.Field.TO))
      // since the aggregated values are 'long' values, we transform them into 'LocalDateTime' values
      .transformGraphHead(
        new TransformLongPropertiesToDateTime<>("earliestStart", "earliestEnd", "lastStart", "lastEnd"))
      // we additionally calculate the average trip duration
      .aggregate(new AverageVertexDuration("avgDuration", TimeDimension.VALID_TIME))
      // since the aggregated values are stored into the graph head, we only need to print them
      .getGraphHead()
      .print();
  }
}
