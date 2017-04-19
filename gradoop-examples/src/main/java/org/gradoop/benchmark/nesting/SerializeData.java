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

/**
 * Contains the programs to execute the benchmarks.
 */
package org.gradoop.benchmark.nesting;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.benchmark.nesting.data.GraphCollectionDelta;
import org.gradoop.benchmark.nesting.data.NestingFilenameConvention;
import org.gradoop.benchmark.nesting.functions.AggregateTheSameEdgeWithinDifferentGraphs;
import org.gradoop.benchmark.nesting.functions.AssociateSourceId;
import org.gradoop.benchmark.nesting.functions.CreateEdge;
import org.gradoop.benchmark.nesting.functions.CreateVertex;
import org.gradoop.benchmark.nesting.functions.ExtractLeftId;
import org.gradoop.benchmark.nesting.functions.SelectImportVertexId;
import org.gradoop.benchmark.nesting.functions.Tuple2StringKeySelector;
import org.gradoop.benchmark.nesting.serializers.SerializeGradoopIdToFile;
import org.gradoop.benchmark.nesting.serializers.SerializePairOfIdsToFile;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;
import org.gradoop.flink.model.impl.operators.nest.NestingBase;
import org.gradoop.flink.model.impl.operators.nest.functions.GraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Initializing the model by writing it down for some further and subsequent tests.
 * This is just an utility class for performing computations and operations
 */
public class SerializeData extends NestingFilenameConvention {

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_SUBGRAPHS_FOLDER = "s";
  /**
   * Path to CSV log file
   */
  private static final String OUTPUT_MODEL = "o";

  /**
   * Logger writing the timing informations to a file.
   */
  private static BufferedWriter LOGGER;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true, "Graph File in gMark format");
    OPTIONS.addOption(OPTION_SUBGRAPHS_FOLDER, "sub", true, "Graph Folder containing subgraphs" +
      " of the main input in gMark format. Each file could be either a vertex file (*-edges.txt) " +
      "or an edge one (*-vertices.txt)");
    OPTIONS.addOption(OUTPUT_MODEL, "out", true, "Path to output in nested format");
  }

  /**
   * Path to input graph data
   */
  private static String INPUT_PATH;
  /**
   * Query to execute.
   */
  private static String SUBGRAPHS;
  /**
   * Path to output
   */
  private static String OUTPATH;

  /**
   * Default constructor for running the tests
   *
   * @param csvPath  File where to store the intermediate results
   * @param basePath Base path where the indexed data is loaded
   */
  @Deprecated
  public SerializeData(String csvPath, String basePath) {
    super(csvPath, basePath);
  }

  /**
   * Main program entry point
   * @param args        Program arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SerializeData.class.getName());
    if (cmd == null) {
      System.exit(1);
    }

    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    SUBGRAPHS = cmd.getOptionValue(OPTION_SUBGRAPHS_FOLDER);
    OUTPATH = cmd.getOptionValue(OUTPUT_MODEL);
    LOGGER =
      new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(
        System.getProperty("user.home") + "/LOGGING.txt")
      ), Charset.forName("UTF-8")));

    GraphCollectionDelta start = extractGraphFromFiles(INPUT_PATH, null, true);

    // Loading the graphs appearing in the graph collection
    // 1. Returning each vertex and edge files association
    File[] filesInDirectory = new File(SUBGRAPHS).listFiles();
    Map<String, List<String>> l = filesInDirectory != null ?
      Arrays.stream(filesInDirectory)
        .map(File::getAbsolutePath)
        .collect(Collectors.groupingBy(x -> x
          .replaceAll("-edges\\.txt$", "")
          .replaceAll("-vertices\\.txt$", ""))) :
      new HashMap<>();

    // 2. Scanning all the files pertaining to the same graph, throught the previous aggregation
    for (List<String> files : l.values()) {
      if (files.size() == 2) {
        String edgeFile = null;
        String vertexFile = null;
        for (String s : files) {
          if (s.endsWith("-edges.txt")) {
            edgeFile = s;
          } else if (s.endsWith("-vertices.txt")) {
            vertexFile = s;
          }
        }
        if (edgeFile != null && vertexFile != null) {
          start = deltaUpdateGraphCollection(start, edgeFile, vertexFile);
        }
      }
    }

    DataSet<Tuple2<String, Vertex>> intermediateVertex = start.getVertices()
      .groupBy(new SelectImportVertexId())
      .combineGroup(new CreateVertex<>(CONFIGURATION.getVertexFactory()));

    DataSet<GraphHead> heads = start.getHeads()
      .map(new Value2Of3<>());

    DataSet<Edge> edges = start.getEdges()
      .distinct(new ExtractLeftId<>())
      .join(intermediateVertex)
      .where(new Tuple2StringKeySelector()).equalTo(0)
      .with(new AssociateSourceId<>())
      .groupBy(0)
      .combineGroup(new AggregateTheSameEdgeWithinDifferentGraphs<>())
      .join(intermediateVertex)
      .where(2).equalTo(0)
      .with(new CreateEdge<>(CONFIGURATION.getEdgeFactory()));

    DataSet<Vertex> vertices = intermediateVertex.map(new Value1Of2<>());

    writeFlattenedGraph(heads, vertices, edges, OUTPATH);
    writeIndexFromSource(true, start, vertices, edges, OUTPATH);
    writeIndexFromSource(false, start, vertices, edges, OUTPATH);
    ENVIRONMENT.execute("DIALER");
    LOGGER.close();
  }

  /**
   *
   * @param isLeftOperand   Check if that is a left operand
   * @param start           Where to extract both the left and right operands information
   * @param vertices        Complete vertices information
   * @param edges           Complete edge information
   * @param file            Path where to write the indices
   * @throws Exception
   */
  public static void writeIndexFromSource(boolean isLeftOperand, GraphCollectionDelta start,
    DataSet<Vertex> vertices, DataSet<Edge> edges, String file) throws Exception {

    LOGGER.newLine();
    NestingIndex generateOperandIndex;

    if (isLeftOperand) {
      generateOperandIndex = NestingBase.createIndex(loadLeftOperand(start, vertices, edges));
    } else {
      generateOperandIndex = NestingBase.createIndex(loadRightOperand(start, vertices, edges));
    }
    writeIndex(generateOperandIndex, generateOperandBasePath(file, isLeftOperand));
  }

  /**
   * Writes an index
   * @param left  Graph index
   * @param s     File path
   */
  private static void writeIndex(NestingIndex left, String s) throws Exception {
    left.getGraphHeads().write(new SerializeGradoopIdToFile(), s + INDEX_HEADERS_SUFFIX);
    left.getGraphVertexMap().write(new SerializePairOfIdsToFile(), s + INDEX_VERTEX_SUFFIX);
    left.getGraphEdgeMap().write(new SerializePairOfIdsToFile(), s + INDEX_EDGE_SUFFIX);

    /*LOGGER.write("Writing Index " + s + " MSECONDS =" + ENVIRONMENT
      .getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS));
    LOGGER.newLine();
    LOGGER.write("indexHeads: " + left.getGraphHeads().count() + " indexVertices: " + left
      .getGraphVertexMap().count() + " indexEdges:" + left.getGraphEdgeMap().count());
    LOGGER.newLine();
    LOGGER.newLine();*/
  }

  /**
   * Writes the flattened version of the graph model
   * @param heads       Heads belonging to the graph
   * @param vertices    Vertices belonging to the graph
   * @param edges       Edges belonging to the graph
   * @param path        Path where to write the file
   * @throws Exception
   */
  protected static void writeFlattenedGraph(DataSet<GraphHead> heads, DataSet<Vertex> vertices,
    DataSet<Edge> edges, String path) throws Exception {

    DataSet<Vertex> flattenedVertices = heads
      .map(new GraphHeadToVertex())
      .union(vertices)
      .distinct(new Id<>());

    LogicalGraph
      lg = LogicalGraph.fromDataSets(heads, flattenedVertices, edges, CONFIGURATION);
    writeLogicalGraph(lg, path, "csv");
    /*SerializeData.LOGGER.write("Writing flattenedGraph MSECONDS =" + ENVIRONMENT
      .getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS));
    SerializeData.LOGGER.write("vertices: " + lg.getVertices().count() + " edges: " +
      lg.getEdges().count() + " heads: " + lg.getGraphHead().count());
    SerializeData.LOGGER.newLine();*/
  }

}
