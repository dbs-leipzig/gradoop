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
package org.gradoop.examples.grouping;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.common.functions.TransformLongPropertiesToDateTime;
import org.gradoop.examples.common.TemporalCitiBikeGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.AverageDuration;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MaxTime;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MinTime;
import org.gradoop.temporal.model.impl.operators.keyedgrouping.TemporalGroupingKeys;

import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Collections;

/**
 * A self contained example on how to use the key-based grouping operator and its support for temporal key
 * functions and aggregations to summarize and analyze temporal graphs.
 */
public class TemporalGroupingExample {

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview of the usage of the {@link KeyedGrouping} operator together with it's
   * temporal and non-temporal key and aggregation functions suitable for temporal analysis.
   *
   * Documentation for all available aggregate functions as well as a detailed description of the
   * keyed grouping can be found in the projects wiki.
   *
   * Using the bike graph in the {@link TemporalCitiBikeGraph} class, the program will:
   * <ol>
   *   <li>group all vertices by their label and week of year</li>
   *   <li>
   *     aggregate the grouped vertices by counting them, calculate the average duration and the first
   *     and last start of it's valid time interval
   *   </li>
   *   <li>group all edges by their label</li>
   *   <li>aggregate the grouped edges by counting them</li>
   * </ol>
   * The resulting grouped graph will be printed to console. For example, one can see how many trips were
   * made per week, how the average trip duration period is, and when the first and last rental started that
   * week. Also the total amount of stations and bikes are given as well as the amount of different bikes used
   * per week.
   *
   * @param args Command line arguments (unused).
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Temporal-Graph-Support#grouping">
   * Gradoop Wiki</a>
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    // Init execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // Load the bike graph
    TemporalGraph bikeGraph = TemporalCitiBikeGraph.getTemporalGraph(GradoopFlinkConfig.createConfig(env));

    // Specify the time dimension we want to consider during our analysis
    TimeDimension dim = TimeDimension.VALID_TIME;

    // Apply the keyed grouping operator
    TemporalGraph groupedGraph = bikeGraph.callForGraph(new KeyedGrouping<>(
        // Vertex grouping keys (label and week of year)
        Arrays.asList(
          GroupingKeys.label(),
          TemporalGroupingKeys.timeStamp(dim, TimeDimension.Field.FROM, ChronoField.ALIGNED_WEEK_OF_YEAR)),
        // Vertex aggregation functions (count, average duration, first and last start)
        Arrays.asList(
          new Count("count"),
          new AverageDuration("avgDur", dim),
          new MinTime("firstStart", dim, TimeDimension.Field.FROM),
          new MaxTime("lastStart", dim, TimeDimension.Field.FROM)),
        // Edge grouping keys (label)
        Collections.singletonList(GroupingKeys.label()),
        // Edge aggregation functions (count)
        Collections.singletonList(new Count())))
      // since the aggregated values are 'long' values, we transform them into 'LocalDateTime' values
      .transformVertices(new TransformLongPropertiesToDateTime<>("firstStart", "lastStart"));

    // print the grouped and aggregated graph
    groupedGraph.print();
  }
}
