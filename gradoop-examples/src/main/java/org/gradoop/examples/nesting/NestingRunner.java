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

package org.gradoop.examples.nesting;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.model.impl.operators.nest.NestingWithDisjunctive;

/**
 * A dedicated program for Graph Nesting over the EPGM model.
 */
public class NestingRunner extends AbstractRunner implements
  ProgramDescription {

  /**
   * Option to declare path to input graph
   */
  public static final String OPTION_TO_BE_NESTED_PATH = "i";
  /**
   * Option to declare path to input graph
   */
  public static final String OPTION_GRAPH_COLLECTION_PATH = "g";
  /**
   * Option to declare path to output graph
   */
  public static final String OPTION_OUTPUT_PATH = "o";

  static {
    OPTIONS.addOption(OPTION_TO_BE_NESTED_PATH, "to-be-nested-graph", true,
      "Path to the folder containing the graph undergoing the nesting operation");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Path to write output files to");
    OPTIONS.addOption(OPTION_GRAPH_COLLECTION_PATH, "graph-collection-path", true,
      "Path to the folder containing the graph collection providing the nesting information");
  }

  /**
   * Main program to run the example. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, NestingRunner.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read arguments from command line
    final String inputPath = cmd.getOptionValue(OPTION_TO_BE_NESTED_PATH);
    final String collectionPath = cmd.getOptionValue(OPTION_GRAPH_COLLECTION_PATH);
    final String outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);

    // initialize graphs
    LogicalGraph graphDatabase = readLogicalGraph(inputPath,  true);
    GraphCollection graphCollection = readGraphCollection(collectionPath, true);

    LogicalGraph nestedGraph = new NestingWithDisjunctive(GradoopId.get())
      .execute(graphDatabase, graphCollection);

    if (nestedGraph != null) {
      writeLogicalGraph(nestedGraph, outputPath);
    } else {
      System.err.println("wrong parameter constellation");
    }
  }

  /**
   * Returns the grouping operator implementation based on the given strategy.
   *
   * @param vertexKey             vertex property key used for grouping
   * @param edgeKey               edge property key used for grouping
   * @param useVertexLabels       use vertex label for grouping, true/false
   * @param useEdgeLabels         use edge label for grouping, true/false
   * @return grouping operator implementation
   */
  private static Grouping getOperator(String vertexKey,
    String edgeKey, boolean useVertexLabels, boolean useEdgeLabels) {
    return new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .addVertexGroupingKey(vertexKey)
      .addEdgeGroupingKey(edgeKey)
      .useVertexLabel(useVertexLabels)
      .useEdgeLabel(useEdgeLabels)
      .addVertexAggregator(new CountAggregator())
      .addEdgeAggregator(new CountAggregator())
      .build();
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_TO_BE_NESTED_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory containing the graph " +
        "that has to be nested" +
        ".");
    }
    if (!cmd.hasOption(OPTION_GRAPH_COLLECTION_PATH)) {
      throw new IllegalArgumentException("Define a graph collection input directory containing " +
        "the nesting information" +
        ".");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Please provide an output path where to store the result" +
        ".");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return NestingRunner.class.getName();
  }
}
