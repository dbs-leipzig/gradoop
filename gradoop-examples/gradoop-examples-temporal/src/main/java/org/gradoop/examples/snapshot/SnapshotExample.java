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
package org.gradoop.examples.snapshot;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.common.TemporalCitiBikeGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.functions.predicates.FromTo;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * A self contained example on how to use the snapshot operator of Gradoop's TPGM model designed to analyse
 * temporal graphs.
 */
public class SnapshotExample {

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview of the usage of the {@code snapshot()} method with pre-defined
   * predicate functions suitable for temporal analysis.
   *
   * Documentation for all available predicate functions as well as a detailed description of the
   * snapshot method can be found in the projects wiki.
   *
   * Using the bike graph in the {@link TemporalCitiBikeGraph} class, the program will:
   * <ol>
   *   <li>extract a snapshot of the temporal graph</li>
   *   <li>verifies the graph by removing dangling edges</li>
   * </ol>
   * The resulting snapshot will be printed to console.
   *
   * @param args Command line arguments (unused).
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Temporal-Graph-Support#snapshot">
   * Gradoop Wiki</a>
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    // init execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // Load the bike graph
    TemporalGraph bikeGraph = TemporalCitiBikeGraph.getTemporalGraph(GradoopFlinkConfig.createConfig(env));

    // the rentals are captured in August 2019 so we define two query timestamps within this month
    LocalDateTime startInterval = LocalDateTime.of(2019, 8, 10, 0, 0, 0);
    LocalDateTime endInterval = startInterval.plus(10, ChronoUnit.DAYS);

    // apply the snapshot operator of type "FROM t1 TO t2"
    TemporalGraph retrievedSnapshot = bikeGraph
      .snapshot(new FromTo(startInterval, endInterval))
      .verify();

    // print graph, which now contains only trips happened between 10th and 20th August 2019
    retrievedSnapshot.print();
  }
}
