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
package org.gradoop.benchmark.nesting.old;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.benchmark.nesting.SerializeData;
import org.gradoop.benchmark.nesting.data.GraphCollectionDelta;
import org.gradoop.benchmark.nesting.functions.AggregateTheSameEdgeWithinDifferentGraphs;
import org.gradoop.benchmark.nesting.functions.AssociateSourceId;
import org.gradoop.benchmark.nesting.functions.CreateEdge;
import org.gradoop.benchmark.nesting.functions.CreateVertex;
import org.gradoop.benchmark.nesting.functions.ExtractLeftId;
import org.gradoop.benchmark.nesting.functions.SelectImportVertexId;
import org.gradoop.benchmark.nesting.functions.Tuple2StringKeySelector;
import org.gradoop.flink.model.impl.operators.nest.ReduceVertexFusion;
import org.gradoop.benchmark.nesting.serializers.Bogus;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class implementing the serialization methods
 */
public class BenchmarkReduceVertexFusion extends GMarkDataReader {

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
  private static final String OUTPUT_EXPERIMENT = "o";

  /**
   * Path to input graph data
   */
  private static String INPUT_PATH;

  /**
   * Query to execute.
   */
  private static String SUBGRAPHS;


  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true, "Graph File in gMark format");
    OPTIONS.addOption(OPTION_SUBGRAPHS_FOLDER, "sub", true, "Graph Folder containing subgraphs" +
      " of the main input in gMark format. Each file could be either a vertex file (*-edges.txt) " +
      "or an edge one (*-vertices.txt)");
    OPTIONS.addOption(OUTPUT_EXPERIMENT, "csv", true, "Where to append the experiment for the " +
      "benchmark");
  }

  /**
   * Indices for the left operand
   */
  private LogicalGraph leftOperand;

  /**
   * Indices for the right operand
   */
  private GraphCollection rightOperand;

  /**
   * Pointer to the Nesting result
   */
  private LogicalGraph result;

  /**
   * Default constructor for running the tests
   * @param benchmarkPath
   */
  public BenchmarkReduceVertexFusion(String basePath, String benchmarkPath) {
    super(basePath, benchmarkPath);
  }

  /**
   * Main program entrance
   * @param args        System arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SerializeData.class.getName());
    if (cmd == null) {
      System.exit(1);
    }

    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    SUBGRAPHS = cmd.getOptionValue(OPTION_SUBGRAPHS_FOLDER);

    BenchmarkReduceVertexFusion
      benchmark = new BenchmarkReduceVertexFusion
      (INPUT_PATH, cmd.getOptionValue(OUTPUT_EXPERIMENT));

    benchmark.registerNextPhase(benchmark::loadOperands, benchmark::finalizeOne);
    benchmark.registerNextPhase(benchmark::runOperator, benchmark::finalizeTwo);
    benchmark.run();
  }

  /**
   * Phase 1: Loads the operands from secondary memory
   */
  private void loadOperands() throws Exception {
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

    leftOperand = loadLeftOperand(start, vertices, edges);
    rightOperand = loadRightOperand(start, vertices, edges);
  }

  /**
   * Phase 2: evaluating the operator
   */
  private void runOperator() {
    result = new ReduceVertexFusion().execute(leftOperand, rightOperand);
  }

  protected void finalizeOne() {
    // Counting each element for the loaded index, alongside with the values of the flattened
    // graph
    leftOperand.getGraphHead().output(new Bogus<>());
    leftOperand.getVertices().output(new Bogus<>());
    leftOperand.getEdges().output(new Bogus<>());
    rightOperand.getGraphHeads().output(new Bogus<>());
    rightOperand.getVertices().output(new Bogus<>());
    rightOperand.getEdges().output(new Bogus<>());
  }

  protected void finalizeTwo() {
    // Counting the computation actually required to produce the result, that is the graph stack
    // Alongside with the resulting indices
    result.getGraphHead().output(new Bogus<>());
    result.getVertices().output(new Bogus<>());
    result.getEdges().output(new Bogus<>());
  }

}
