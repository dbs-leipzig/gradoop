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

package org.gradoop.examples.grouping;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;

/**
 * A dedicated program for parametrized graph grouping.
 */
public class GroupingRunner extends AbstractRunner implements
  ProgramDescription {

  /**
   * Option to declare path to input graph
   */
  public static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  public static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Vertex grouping key option
   */
  public static final String OPTION_VERTEX_GROUPING_KEY = "vgk";
  /**
   * Edge grouping key option
   */
  public static final String OPTION_EDGE_GROUPING_KEY = "egk";
  /**
   * Use vertex label option
   */
  public static final String OPTION_USE_VERTEX_LABELS = "uvl";
  /**
   * Use edge label option
   */
  public static final String OPTION_USE_EDGE_LABELS = "uel";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "vertex-input-path", true,
      "Path to vertex file");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Path to write output files to");
    OPTIONS.addOption(OPTION_VERTEX_GROUPING_KEY, "vertex-grouping-key", true,
      "EPGMProperty key to group vertices on.");
    OPTIONS.addOption(OPTION_EDGE_GROUPING_KEY, "edge-grouping-key", true,
      "EPGMProperty key to group edges on.");
    OPTIONS.addOption(OPTION_USE_VERTEX_LABELS, "use-vertex-labels", false,
      "Group on vertex labels");
    OPTIONS.addOption(OPTION_USE_EDGE_LABELS, "use-edge-labels", false,
      "Group on edge labels");
  }

  /**
   * Main program to run the example. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, GroupingRunner.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read arguments from command line
    final String inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    final String outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);

    boolean useVertexKey = cmd.hasOption(OPTION_VERTEX_GROUPING_KEY);
    String vertexKey =
      useVertexKey ? cmd.getOptionValue(OPTION_VERTEX_GROUPING_KEY) : null;
    boolean useEdgeKey = cmd.hasOption(OPTION_EDGE_GROUPING_KEY);
    String edgeKey =
      useEdgeKey ? cmd.getOptionValue(OPTION_EDGE_GROUPING_KEY) : null;
    boolean useVertexLabels = cmd.hasOption(OPTION_USE_VERTEX_LABELS);
    boolean useEdgeLabels = cmd.hasOption(OPTION_USE_EDGE_LABELS);

    // initialize EPGM database
    LogicalGraph graphDatabase = readLogicalGraph(inputPath, false);

    // initialize grouping method
    Grouping grouping = getOperator(
      vertexKey, edgeKey, useVertexLabels, useEdgeLabels);
    // call grouping on whole database graph
    LogicalGraph summarizedGraph = graphDatabase.callForGraph(grouping);

    if (summarizedGraph != null) {
      writeLogicalGraph(summarizedGraph, outputPath);
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
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_VERTEX_GROUPING_KEY) &&
      !cmd.hasOption(OPTION_USE_VERTEX_LABELS)) {
      throw new IllegalArgumentException(
        "Chose at least a vertex grouping key or use vertex labels.");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return GroupingRunner.class.getName();
  }
}
