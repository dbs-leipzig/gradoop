/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.benchmark.nesting.data;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.benchmark.nesting.SerializeData;
import org.gradoop.benchmark.nesting.serializers.Bogus;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.NestingBase;

/**
 * Abstracting the benchmark operation over all the possible implementations
 */
public abstract class AbstractBenchmark extends NestingFilenameConvention implements PhaseDoer {

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";

  /**
   * Path to CSV log file
   */
  private static final String OUTPUT_EXPERIMENT = "o";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true, "Graph File in the serialized format");
    OPTIONS.addOption(OUTPUT_EXPERIMENT, "csv", true,
      "Where to append the experiment for the " + "benchmark");
  }

  /**
   * Default constructor for running the tests
   *
   * @param basePath Base path where the indexed data is loaded
   * @param csvPath  File where to store the intermediate results
   */
  public AbstractBenchmark(String basePath, String csvPath) {
    super(basePath, csvPath);
  }

  /**
   * Starts running the benchmarks from the classes extending this one.
   * @param clazz       Class to initialize and where to parse the information.
   * @param args        Arguments
   * @param <T>         Class extending AbstractBenchmark
   * @throws Exception
   */
  public static <T extends AbstractBenchmark> void runBenchmark(Class<T> clazz, String[] args)
    throws Exception {
    CommandLine cmd = parseArguments(args, SerializeData.class.getName());
    if (cmd == null) {
      System.exit(1);
    }
    T runner = clazz.getConstructor(String.class, String.class)
                    .newInstance(cmd.getOptionValue(OPTION_INPUT_PATH),
                                 cmd.getOptionValue(OUTPUT_EXPERIMENT));
    runner.performOperation();
    runner.benchmarkOperation();
    runner.benchmark(0);
  }

  public void registerLogicalGraph(LogicalGraph counter) throws Exception {
    register(counter.getGraphHead(), "HEAD", 0);
    register(counter.getVertices(), "VERTICES", 1);
    register(counter.getEdges(), "EDGES", 2);
  }

}
