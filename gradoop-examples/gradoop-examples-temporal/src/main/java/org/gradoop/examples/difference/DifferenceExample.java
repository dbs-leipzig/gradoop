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
package org.gradoop.examples.difference;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.common.TemporalCitiBikeGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.functions.predicates.FromTo;

import java.time.LocalDateTime;

/**
 * A self contained example on how to use the difference operator of Gradoop's TPGM model designed to analyse
 * temporal graphs.
 */
public class DifferenceExample {

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview of the usage of the
   * {@link org.gradoop.temporal.model.impl.operators.diff.Diff} method with pre-defined predicate
   * functions suitable for temporal analysis.
   *
   * Documentation for all available predicate functions as well as a detailed description of the
   * diff method can be found in the projects wiki.
   *
   * Using the bike graph in the {@link TemporalCitiBikeGraph} class, the program will:
   * <ol>
   *   <li>compare two snapshots of the temporal graph</li>
   *   <li>verifies the graph by removing dangling edges</li>
   * </ol>
   * The result is a graph composed by the union of both snapshots where each element is annotated with an
   * additional property. The value of that property indicates an addition (1), a deletion (-1) or a
   * persistence (0) of the element.
   *
   * @param args Command line arguments (unused).
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Temporal-Graph-Support#difference">
   * Gradoop Wiki</a>
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    // init execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // Load the bike graph
    TemporalGraph bikeGraph = TemporalCitiBikeGraph.getTemporalGraph(GradoopFlinkConfig.createConfig(env));

    // the rentals are captured in August 2019 so we define two query intervals to compare
    LocalDateTime firstQueryPeriodStart = LocalDateTime.of(2019, 8, 1, 0, 0, 0);
    LocalDateTime firstQueryPeriodEnd = LocalDateTime.of(2019, 8, 16, 0, 0, 0);

    LocalDateTime secondQueryPeriodStart = LocalDateTime.of(2019, 8, 10, 0, 0, 0);
    LocalDateTime secondQueryPeriodEnd = LocalDateTime.of(2019, 9, 1, 0, 0, 0);

    // apply the diff operator with two predicates of type "FROM t1 TO t2"
    TemporalGraph annotatedDiffGraph = bikeGraph
      .diff(
        new FromTo(firstQueryPeriodStart, firstQueryPeriodEnd),
        new FromTo(secondQueryPeriodStart, secondQueryPeriodEnd))
      .verify();

    // print graph, which now contains the union of both snapshots with annotated elements
    // _diff: 0 (in both snapshots); _diff: 1 (only in second snapshot); _diff: -1 (only in first snapshot)
    annotatedDiffGraph.print();
  }
}
