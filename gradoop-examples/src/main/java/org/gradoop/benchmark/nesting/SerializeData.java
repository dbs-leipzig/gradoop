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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.log4j.varia.NullAppender;
import org.gradoop.benchmark.nesting.functions.*;
import org.gradoop.benchmark.nesting.model.GraphCollectionDelta;
import org.gradoop.benchmark.nesting.serializers.SerializeGradoopIdToFile;
import org.gradoop.benchmark.nesting.serializers.SerializePairOfIdsToFile;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;
import org.gradoop.flink.model.impl.operators.nest.NestingBase;
import org.gradoop.flink.model.impl.operators.nest.functions.ConstantZero;
import org.gradoop.flink.model.impl.operators.nest.functions.GraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Initializing the model by writing it down for some further and subsequent tests
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
   * Global environment
   */
  private final static ExecutionEnvironment ENVIRONMENT;

  /**
   * GradoopFlink configuration
   */
  private static GradoopFlinkConfig CONFIGURATION;

  private static BufferedWriter LOGGER;

  static {
    ENVIRONMENT = getExecutionEnvironment();
    CONFIGURATION = GradoopFlinkConfig.createConfig(ENVIRONMENT);
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SerializeData.class.getName());
    if (cmd == null) {
      System.exit(1);
    }

    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    SUBGRAPHS = cmd.getOptionValue(OPTION_SUBGRAPHS_FOLDER);
    OUTPATH = cmd.getOptionValue(OUTPUT_MODEL);
    LOGGER =
      new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(OUTPATH
        +"LOGGING.txt")
      )));

    GraphCollectionDelta start =
      extractGraphFromFiles(INPUT_PATH, null, true);

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
      .distinct(new ExtractLeftId())
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
    LOGGER.close();
  }

  public static void writeIndexFromSource(boolean isLeftOperand, GraphCollectionDelta start,
    DataSet<Vertex> vertices, DataSet<Edge> edges, String file) throws Exception {
    DataSet<GraphHead> operandHeads = start.getHeads()
      .filter(new Value1Of3AsFilter(isLeftOperand))
      .map(new Value2Of3<>());

    DataSet<Vertex> operandVertices = vertices
      .joinWithTiny(operandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    DataSet<Edge> operandEdges = edges
      .joinWithTiny(operandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    LOGGER.newLine();
    NestingIndex leftIndex;

    if (isLeftOperand) {
      leftIndex = NestingBase.createIndex(LogicalGraph.fromDataSets(operandHeads,
        operandVertices,
        operandEdges, CONFIGURATION));
    } else {
      leftIndex = NestingBase.createIndex(GraphCollection.fromDataSets(operandHeads,
        operandVertices,
        operandEdges, CONFIGURATION));
    }
    writeIndex(leftIndex, generateOperandBasePath(file, isLeftOperand));
  }

  /**
   * Writes the flattened version of the graph model
   * @param heads
   * @param vertices
   * @param edges
   * @param path
   * @throws Exception
   */
  private static void writeFlattenedGraph(DataSet<GraphHead> heads, DataSet<Vertex> vertices,
    DataSet<Edge> edges, String path) throws Exception {

    DataSet<Vertex> flattenedVertices = heads
      .map(new GraphHeadToVertex())
      .union(vertices)
      .distinct(new Id<>());

    LogicalGraph lg = LogicalGraph.fromDataSets(heads, flattenedVertices, edges, CONFIGURATION);
    writeLogicalGraph(lg, path);
    LOGGER.write("Writing flattenedGraph MSECONDS =" + ENVIRONMENT
      .getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS));
    LOGGER.write("vertices: "+ lg.getVertices().count() + " edges: "+ lg.getEdges().count() + " " +
      "heads: "+ lg.getGraphHead().count());
    LOGGER.newLine();
  }

  /**
   * Extracts the partial graph definition form files
   * @param edges_global_file       Edges file
   * @param vertices_global_file    Vertices file
   * @param isLeftOperand           If the operand represented is the left one
   * @return  Instance of the operand
   */
  private static GraphCollectionDelta extractGraphFromFiles
    (String edges_global_file, String vertices_global_file, boolean isLeftOperand) {
    // This information is going to be used when serializing the operands
    DataSet<Tuple3<String, Boolean, GraphHead>> heads =
      ENVIRONMENT.fromElements(edges_global_file)
        .map(new AssociateFileToGraph(isLeftOperand, CONFIGURATION.getGraphHeadFactory()));

    // Extracting the head id. Required to create a LogicalGraph
    DataSet<GradoopId> head = heads.map(new TripleWithGraphHeadToId());

    // Edges with graph association
    DataSet<Tuple2<ImportEdge<String>, GradoopId>> edges =
      ENVIRONMENT.readTextFile(edges_global_file)
        .flatMap(new StringAsEdge())
        .cross(head);

    // Vertices with graph association
    DataSet<Tuple2<ImportVertex<String>, GradoopId>> vertices =
      edges
        .flatMap(new ImportEdgeToVertex());

    if (vertices_global_file != null) {
      vertices = ENVIRONMENT.readTextFile(vertices_global_file)
        .map(new StringAsVertex())
        .cross(head)
        .union(vertices);
    }
    return new GraphCollectionDelta(heads, vertices, edges);
  }

  /**
   * Updates the graph definition
   * @param delta                 Previous updated version
   * @param edges_global_file     File defining the edges
   * @param vertices_global_file  File defining the vertices
   * @return                      Updates the graph definition
   */
  private static GraphCollectionDelta deltaUpdateGraphCollection(GraphCollectionDelta delta,
    String edges_global_file, String vertices_global_file) {

    GraphCollectionDelta deltaPlus = extractGraphFromFiles(edges_global_file,
      vertices_global_file, false);

    return new GraphCollectionDelta(delta.getHeads().union(deltaPlus.getHeads()),
      delta.getVertices().union(deltaPlus.getVertices()),
      delta.getEdges().union(deltaPlus.getEdges()));
  }

  /**
   * Writes an index
   * @param left  Graph index
   * @param s     File path
   */
  private static void writeIndex(NestingIndex left, String s) throws Exception {
    left.getGraphHeads().write(new SerializeGradoopIdToFile(), s + INDEX_HEADERS_SUFFIX);
    left.getGraphHeadToVertex().write(new SerializePairOfIdsToFile(), s + INDEX_VERTEX_SUFFIX);
    left.getGraphHeadToEdge().write(new SerializePairOfIdsToFile(), s + INDEX_EDGE_SUFFIX);

    ENVIRONMENT.execute();
    LOGGER.write("Writing Index " + s + " MSECONDS =" + ENVIRONMENT
      .getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS));
    LOGGER.newLine();
    LOGGER.write("indexHeads: "+ left.getGraphHeads().count()+" indexVertices: "+ left
      .getGraphHeadToVertex().count() +" indexEdges:" + left.getGraphHeadToEdge().count());
    LOGGER.newLine();
    LOGGER.newLine();
  }


}
