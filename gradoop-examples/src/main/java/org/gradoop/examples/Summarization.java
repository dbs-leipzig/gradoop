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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.examples;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.FlinkGraphStore;
import org.gradoop.model.store.EPGraphStore;

public class Summarization implements ProgramDescription {

  public static final String OPTION_VERTEX_INPUT_PATH = "vip";
  public static final String OPTION_EDGE_INPUT_PATH = "eip";
  public static final String OPTION_OUTPUT_PATH = "op";
  public static final String OPTION_VERTEX_GROUPING_KEY = "vgk";
  public static final String OPTION_EDGE_GROUPING_KEY = "egk";
  public static final String OPTION_USE_VERTEX_LABELS = "uvl";
  public static final String OPTION_USE_EDGE_LABELS = "uel";

  private static Options OPTIONS;

  static {
    OPTIONS = new Options();
    OPTIONS.addOption(OPTION_VERTEX_INPUT_PATH, "vertex-input-path", true,
      "Path to vertex file");
    OPTIONS.addOption(OPTION_EDGE_INPUT_PATH, "edge-input-path", true,
      "Path to edge file");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Path to write output files to");
    OPTIONS.addOption(OPTION_VERTEX_GROUPING_KEY, "vertex-grouping-key", true,
      "Property key to group vertices on.");
    OPTIONS.addOption(OPTION_EDGE_GROUPING_KEY, "edge-grouping-key", true,
      "Property key to group edges on.");
    OPTIONS.addOption(OPTION_USE_VERTEX_LABELS, "use-vertex-labels", false,
      "Summarize on vertex labels");
    OPTIONS.addOption(OPTION_USE_EDGE_LABELS, "use-edge-labels", false,
      "Summarize on edge labels");
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args);
    if (cmd == null) {
      return;
    }

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    final String vertexInputPath = cmd.getOptionValue(OPTION_VERTEX_INPUT_PATH);
    final String edgeInputPath = cmd.getOptionValue(OPTION_EDGE_INPUT_PATH);

    EPGraphStore graphStore =
      FlinkGraphStore.fromJsonFile(vertexInputPath, edgeInputPath, env);

    EPGraph databaseGraph = graphStore.getDatabaseGraph();

    boolean useVertexKey = cmd.hasOption(OPTION_VERTEX_GROUPING_KEY);
    String vertexKey =
      (useVertexKey) ? cmd.getOptionValue(OPTION_VERTEX_GROUPING_KEY) : null;
    boolean useEdgeKey = cmd.hasOption(OPTION_EDGE_GROUPING_KEY);
    String edgeKey =
      (useEdgeKey) ? cmd.getOptionValue(OPTION_EDGE_GROUPING_KEY) : null;
    boolean useVertexLabels = cmd.hasOption(OPTION_USE_VERTEX_LABELS);
    boolean useEdgeLabels = cmd.hasOption(OPTION_USE_EDGE_LABELS);

    EPGraph summarizedGraph = null;
    if (useVertexLabels && useEdgeLabels && useVertexKey && useEdgeKey) {
      summarizedGraph =
        databaseGraph.summarizeOnVertexAndEdgeLabel(vertexKey, edgeKey);
    } else if (useVertexLabels && useEdgeLabels && useVertexKey) {
      summarizedGraph =
        databaseGraph.summarizeOnVertexAndEdgeLabelAndVertexProperty(vertexKey);
    } else if (useVertexLabels && useEdgeLabels && useEdgeKey) {
      summarizedGraph =
        databaseGraph.summarizeOnVertexAndEdgeLabelAndEdgeProperty(edgeKey);
    } else if (useVertexLabels && useEdgeLabels) {
      summarizedGraph = databaseGraph.summarizeOnVertexAndEdgeLabel();
    } else if (useVertexLabels && useVertexKey && useEdgeKey) {
      summarizedGraph =
        databaseGraph.summarizeOnVertexLabel(vertexKey, edgeKey);
    } else if (useVertexLabels && useVertexKey) {
      summarizedGraph =
        databaseGraph.summarizeOnVertexLabelAndVertexProperty(vertexKey);
    } else if (useVertexLabels && useEdgeKey) {
      summarizedGraph =
        databaseGraph.summarizeOnVertexLabelAndEdgeProperty(edgeKey);
    } else if (useVertexLabels) {
      summarizedGraph = databaseGraph.summarizeOnVertexLabel();
    } else if (useVertexKey && useEdgeKey) {
      summarizedGraph = databaseGraph.summarize(vertexKey, edgeKey);
    } else if (useVertexKey) {
      summarizedGraph = databaseGraph.summarize(vertexKey);
    }
    if (summarizedGraph != null) {
      if (cmd.hasOption(OPTION_OUTPUT_PATH)) {
        writeOutputFiles(summarizedGraph,
          cmd.getOptionValue(OPTION_OUTPUT_PATH));
      } else {
        System.out.println(summarizedGraph.toString());
      }
    } else {
      System.err.println("wrong parameter constellation");
    }
  }

  private static void writeOutputFiles(EPGraph graph, String outputPath) throws
    Exception {
    final String fileSeparator = System.getProperty("file.separator");
    final String vertexFile =
      String.format("%s%s%s", outputPath, fileSeparator, "nodes.json");
    final String edgeFile =
      String.format("%s%s%s", outputPath, fileSeparator, "edges.json");
    final String graphFile =
      String.format("%s%s%s", outputPath, fileSeparator, "graphs.json");
    graph.writeAsJson(vertexFile, edgeFile, graphFile);
  }

  private static CommandLine parseArguments(String[] args) throws
    ParseException {
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(Summarization.class.getName(), OPTIONS, true);
      return null;
    }
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(OPTIONS, args);

    performSanityCheck(cmd);

    return cmd;
  }

  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_VERTEX_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a vertex input path.");
    }
    if (!cmd.hasOption(OPTION_EDGE_INPUT_PATH)) {
      throw new IllegalArgumentException("Define an edge input path.");
    }
    if (!cmd.hasOption(OPTION_VERTEX_GROUPING_KEY) &&
      !cmd.hasOption(OPTION_USE_VERTEX_LABELS)) {
      throw new IllegalArgumentException(
        "Chose at least a vertex grouping key or use vertex labels.");
    }
  }

  @Override
  public String getDescription() {
    return "Summarization";
  }
}
