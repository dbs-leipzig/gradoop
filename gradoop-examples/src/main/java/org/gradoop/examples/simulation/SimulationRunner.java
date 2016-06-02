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

package org.gradoop.examples.simulation;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.matching.simulation.dual.DualSimulation;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

import java.util.concurrent.TimeUnit;

/**
 * Performs graph pattern matching using simulation on an arbitrary input graph.
 */
public class SimulationRunner extends AbstractRunner
  implements ProgramDescription {

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH  = "i";
  /**
   *Option to declare path to output graph
    */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * GDL query string
   */
  private static final String OPTION_QUERY_GRAPH = "q";
  /**
   * Attach original data true/false
   */
  private static final String OPTION_ATTACH_DATA = "a";
  /**
   * Use bulk iteration true/false
    */
  private static final String OPTION_USE_BULK    = "b";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input-path", true,
      "Input graph directory (EPGM json format)");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Output graph directory");
    OPTIONS.addOption(OPTION_QUERY_GRAPH, "query", true,
      "GDL based query graph");
    OPTIONS.addOption(OPTION_ATTACH_DATA, "attach-data", false,
      "Attach original vertex and edge data to the match graph");
    OPTIONS.addOption(OPTION_USE_BULK, "bulk-iteration", false,
      "Use bulk iteration for simulation kernel (delta otherwise)");
  }

  /**
   * Runs the simulation.
   *
   * @param args args[0]: input dir, args[1]: output dir
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SimulationRunner.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    String inputDir     = cmd.getOptionValue(OPTION_INPUT_PATH);
    String outputDir    = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    String query        = cmd.getOptionValue(OPTION_QUERY_GRAPH);
    boolean attachData  = cmd.hasOption(OPTION_ATTACH_DATA);
    boolean useBulk     = cmd.hasOption(OPTION_USE_BULK);

    EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo> epgmDatabase =
      readEPGMDatabase(inputDir);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      execute(epgmDatabase.getDatabaseGraph(), query, attachData, useBulk);

    writeGraphCollection(result, outputDir);

    System.out.println(String.format("Net runtime [s]: %d",
      getExecutionEnvironment()
        .getLastJobExecutionResult()
        .getNetRuntime(TimeUnit.SECONDS)));
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Define an graph output directory.");
    }
    if (!cmd.hasOption(OPTION_QUERY_GRAPH)) {
      throw new IllegalArgumentException("Define a graph query.");
    }
  }

  /**
   * Executes dual simulation on the given logical graph.
   *
   * @param databaseGraph data graph
   * @param query         query graph
   * @param attachData    attach vertex and edge data to the match graph
   * @param useBulk       use bulk iteration instead of delta iteration
   * @return result match graph
   */
  private static GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> execute(
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> databaseGraph,
    String query, boolean attachData, boolean useBulk) {

    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query, attachData, useBulk);

    return op.execute(databaseGraph);
  }

  @Override
  public String getDescription() {
    return SimulationRunner.class.getName();
  }
}
