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

package org.gradoop.examples.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.grouping.Grouping;
import org.gradoop.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.model.impl.operators.grouping.functions.aggregation.MaxAggregator;
import org.gradoop.model.impl.operators.grouping.functions.aggregation.MinAggregator;
import org.gradoop.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized graph grouping.
 */
public class GroupingBenchmark extends AbstractRunner
  implements ProgramDescription {

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
  /**
   * Path to CSV log file
   */
  public static final String OPTION_CSV_PATH = "csv";
  /**
   * Use aggregator (min, max, count, none)
   */
  private static final String OPTION_AGGREGATION = "agg";
  /**
   * Used VertexKey for grouping
   */
  private static String VERTEX_KEYS;
  /**
   * Used EdgeKey for grouping
   */
  private static String EDGE_KEYS;
  /**
   * Used csv path
   */
  private static String CSV_PATH;
  /**
   * Used hdfs INPUT_PATH
   */
  private static String INPUT_PATH;
  /**
   * Used hdfs OUTPUT_PATH
   */
  private static String OUTPUT_PATH;
  /**
   * Uses VertexLabels
   */
  private static boolean USE_VERTEX_LABELS;
  /**
   * Uses EdgeLabels
   */
  private static boolean USE_EDGE_LABELS;


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
    OPTIONS.addOption(OPTION_CSV_PATH, "csv-path", true, "Path of the " +
      "generated CSV-File");
    OPTIONS.addOption(OPTION_AGGREGATION, "aggregation", true, "Applied " +
      "aggregation function");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, GroupingBenchmark.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read arguments from command line
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH);

    boolean useVertexKey = cmd.hasOption(OPTION_VERTEX_GROUPING_KEY);
    VERTEX_KEYS =
      useVertexKey ? cmd.getOptionValue(OPTION_VERTEX_GROUPING_KEY) : null;
    boolean useEdgeKey = cmd.hasOption(OPTION_EDGE_GROUPING_KEY);
    EDGE_KEYS =
      useEdgeKey ? cmd.getOptionValue(OPTION_EDGE_GROUPING_KEY) : null;
    USE_VERTEX_LABELS = cmd.hasOption(OPTION_USE_VERTEX_LABELS);
    USE_EDGE_LABELS = cmd.hasOption(OPTION_USE_EDGE_LABELS);
    CSV_PATH = cmd.getOptionValue(OPTION_CSV_PATH);

    String aggV =  cmd.getOptionValue(OPTION_AGGREGATION);

    // set aggregator
    PropertyValueAggregator agg;

    switch (aggV) {
    case "count" :
      agg = new CountAggregator();
      break;
    case "max" :
      agg = new MaxAggregator("*", "count");
      break;
    case "min" :
      agg = new MinAggregator("*", "count");
      break;
    default:
      agg = null;
      break;
    }

    // initialize EPGM database
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graphDatabase =
      readLogicalGraph(INPUT_PATH, false);

    VERTEX_KEYS = VERTEX_KEYS.replaceAll("\\s", "");
    EDGE_KEYS = EDGE_KEYS.replaceAll("\\s", "");
    List<String> vertexKeys = Arrays.asList(VERTEX_KEYS.split(","));
    List<String> edgeKeys = Arrays.asList(EDGE_KEYS.split(","));

    // build grouping operator
    Grouping grouping = getOperator(
      vertexKeys, edgeKeys, USE_VERTEX_LABELS, USE_EDGE_LABELS, agg);

    // call grouping on whole database graph
    LogicalGraph summarizedGraph = graphDatabase.callForGraph(grouping);
    if (summarizedGraph != null) {
      writeLogicalGraph(summarizedGraph, OUTPUT_PATH);
      writeCSV();
    } else {
      System.err.println("wrong parameter constellation");
    }

  }

  /**
   * Returns the grouping operator implementation based on the given strategy.
   *
   * @param vertexKeys            vertex property keys used for grouping
   * @param edgeKeys              edge property keys used for grouping
   * @param useVertexLabels       use vertex label for grouping, true/false
   * @param useEdgeLabels         use edge label for grouping, true/false
   * @param agg                   used aggregator
   * @return grouping operator implementation
   */
  private static Grouping getOperator(List<String> vertexKeys,
    List<String> edgeKeys, boolean useVertexLabels, boolean useEdgeLabels,
    PropertyValueAggregator agg) {
    Grouping.GroupingBuilder builder =
      new Grouping.GroupingBuilder()
        .setStrategy(GroupingStrategy.GROUP_REDUCE)
        .useVertexLabel(useVertexLabels)
        .useEdgeLabel(useEdgeLabels);

    if (agg != null) {
      builder
        .addVertexAggregator(agg)
        .addEdgeAggregator(agg);
    }

    for (String vKey : vertexKeys) {
      builder.addVertexGroupingKey(vKey);
    }

    for (String eKey : edgeKeys) {
      builder.addEdgeGroupingKey(eKey);
    }
    return builder.build();

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
    if (!cmd.hasOption(OPTION_CSV_PATH)) {
      throw new IllegalArgumentException("Path to CSV-File need to be set");
    }
    if (!cmd.hasOption(OPTION_VERTEX_GROUPING_KEY) &&
      !cmd.hasOption(OPTION_USE_VERTEX_LABELS)) {
      throw new IllegalArgumentException(
        "Chose at least one vertex grouping key or use vertex labels.");
    }
    if (cmd.hasOption(OPTION_AGGREGATION)) {
      String aggV = cmd.getOptionValue(OPTION_AGGREGATION);
      Set<String> validAggs = new HashSet<>(
        Arrays.asList("min", "max", "count", "none"));
      if (!validAggs.contains(aggV)) {
        throw new IllegalArgumentException(
          "Can't recognize aggregation function. Valid parameters are: " +
            "{count, min, max, none}."
        );
      }
    }
  }

  /**
   * Method to create and add lines to a csv-file
   * @throws IOException
   */
  private static void writeCSV() throws IOException {

    String head = String.format("%s|%s|%s|%s|%s|%s|%s%n", "Parallelism",
      "dataset", "vertexKeys", "edgeKeys", "USE_VERTEX_LABELS",
      "USE_EDGE_LABELS", "Runtime(s)");

    String tail = String.format("%s|%s|%s|%s|%s|%s|%s%n",
      getExecutionEnvironment().getParallelism(), INPUT_PATH,
      VERTEX_KEYS, EDGE_KEYS, USE_VERTEX_LABELS, USE_EDGE_LABELS,
      getExecutionEnvironment().getLastJobExecutionResult()
        .getNetRuntime(TimeUnit.SECONDS));

    File f = new File(CSV_PATH);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(CSV_PATH, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return GroupingBenchmark.class.getName();
  }
}
