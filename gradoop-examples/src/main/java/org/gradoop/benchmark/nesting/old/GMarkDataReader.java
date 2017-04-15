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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.benchmark.nesting.NestingFilenameConvention;
import org.gradoop.benchmark.nesting.SerializeData;
import org.gradoop.benchmark.nesting.data.GraphCollectionDelta;
import org.gradoop.benchmark.nesting.functions.AssociateFileToGraph;
import org.gradoop.benchmark.nesting.functions.ImportEdgeToVertex;
import org.gradoop.benchmark.nesting.functions.SelectElementsInHeads;
import org.gradoop.benchmark.nesting.functions.StringAsEdge;
import org.gradoop.benchmark.nesting.functions.StringAsVertex;
import org.gradoop.benchmark.nesting.functions.TripleWithGraphHeadToId;
import org.gradoop.benchmark.nesting.functions.Value1Of3AsFilter;
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
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;
import org.gradoop.flink.model.impl.operators.nest.NestingBase;
import org.gradoop.flink.model.impl.operators.nest.functions.ConstantZero;
import org.gradoop.flink.model.impl.operators.nest.functions.GraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;

import java.io.BufferedWriter;
import java.util.concurrent.TimeUnit;

/**
 * Created by vasistas on 12/04/17.
 */
public abstract class GMarkDataReader extends NestingFilenameConvention {

  /**
   * Logger writing the timing informations to a file.
   */
  protected static BufferedWriter LOGGER;

  /**
   * Default constructor for running the tests
   *
   * @param csvPath  File where to store the intermediate results
   * @param basePath Base path where the indexed data is loaded
   */
  public GMarkDataReader(String basePath, String csvPath) {
    super(basePath, csvPath);
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
    SerializeData.LOGGER.write("Writing flattenedGraph MSECONDS =" + ENVIRONMENT
      .getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS));
    SerializeData.LOGGER.write("vertices: " + lg.getVertices().count() + " edges: " +
      lg.getEdges().count() + " heads: " + lg.getGraphHead().count());
    SerializeData.LOGGER.newLine();
  }

  public static LogicalGraph loadLeftOperand(GraphCollectionDelta start, DataSet<Vertex> vertices,
    DataSet<Edge> edges) throws Exception {
    DataSet<GraphHead> operandHeads = start.getHeads()
      .filter(new Value1Of3AsFilter(true))
      .map(new Value2Of3<>());

    DataSet<Vertex> operandVertices = vertices
      .joinWithTiny(operandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    DataSet<Edge> operandEdges = edges
      .joinWithTiny(operandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    return LogicalGraph.fromDataSets(operandHeads, operandVertices, operandEdges, CONFIGURATION);
  }

  public static GraphCollection loadRightOperand(GraphCollectionDelta start,
    DataSet<Vertex> vertices, DataSet<Edge> edges) throws Exception {
    DataSet<GraphHead> operandHeads = start.getHeads()
      .filter(new Value1Of3AsFilter(true))
      .map(new Value2Of3<>());

    DataSet<Vertex> operandVertices = vertices
      .joinWithTiny(operandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    DataSet<Edge> operandEdges = edges
      .joinWithTiny(operandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    return GraphCollection.fromDataSets(operandHeads, operandVertices, operandEdges, CONFIGURATION);
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
      generateOperandIndex = NestingBase.createIndex(loadRightOperand(start, vertices, edges))
      ;
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

    ENVIRONMENT.execute();
    LOGGER.write("Writing Index " + s + " MSECONDS =" + ENVIRONMENT
      .getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS));
    LOGGER.newLine();
    LOGGER.write("indexHeads: " + left.getGraphHeads().count() + " indexVertices: " + left
      .getGraphVertexMap().count() + " indexEdges:" + left.getGraphEdgeMap().count());
    LOGGER.newLine();
    LOGGER.newLine();
  }

  /**
   * Updates the graph definition
   * @param delta                 Previous updated version
   * @param edgesGlobalFile     File defining the edges
   * @param verticesGlobalFile  File defining the vertices
   * @return                      Updates the graph definition
   */
  protected static GraphCollectionDelta deltaUpdateGraphCollection(GraphCollectionDelta delta,
    String edgesGlobalFile, String verticesGlobalFile) {

    GraphCollectionDelta deltaPlus = extractGraphFromFiles(edgesGlobalFile,
      verticesGlobalFile, false);

    return new GraphCollectionDelta(delta.getHeads().union(deltaPlus.getHeads()),
      delta.getVertices().union(deltaPlus.getVertices()),
      delta.getEdges().union(deltaPlus.getEdges()));
  }

  /**
   * Extracts the partial graph definition form files
   * @param edgesGlobalFile       Edges file
   * @param verticesGlobalFile    Vertices file
   * @param isLeftOperand           If the operand represented is the left one
   * @return  Instance of the operand
   */
  protected static GraphCollectionDelta extractGraphFromFiles(String edgesGlobalFile,
    String verticesGlobalFile, boolean isLeftOperand) {
    // This information is going to be used when serializing the operands
    DataSet<Tuple3<String, Boolean, GraphHead>> heads =
      ENVIRONMENT.fromElements(edgesGlobalFile)
        .map(new AssociateFileToGraph(isLeftOperand, CONFIGURATION.getGraphHeadFactory()));

    // Extracting the head id. Required to create a LogicalGraph
    DataSet<GradoopId> head = heads.map(new TripleWithGraphHeadToId());

    // Edges with graph association
    DataSet<Tuple2<ImportEdge<String>, GradoopId>> edges =
      ENVIRONMENT.readTextFile(edgesGlobalFile)
        .flatMap(new StringAsEdge())
        .cross(head);

    // Vertices with graph association
    DataSet<Tuple2<ImportVertex<String>, GradoopId>> vertices =
      edges
        .flatMap(new ImportEdgeToVertex<>());

    if (verticesGlobalFile != null) {
      vertices = ENVIRONMENT.readTextFile(verticesGlobalFile)
        .map(new StringAsVertex())
        .cross(head)
        .union(vertices);
    }
    return new GraphCollectionDelta(heads, vertices, edges);
  }
}
