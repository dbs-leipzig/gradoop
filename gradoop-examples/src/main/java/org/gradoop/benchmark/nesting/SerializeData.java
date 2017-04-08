package org.gradoop.benchmark.nesting;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggerRepository;
import org.gradoop.benchmark.nesting.functions.*;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
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
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerAdapter;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by vasistas on 08/04/17.
 */
public class SerializeData extends AbstractRunner {

  public final static ExecutionEnvironment ENVIRONMENT;
  public final static GradoopFlinkConfig CONFIGURATION;

  static {
    ENVIRONMENT = getExecutionEnvironment();
    CONFIGURATION = GradoopFlinkConfig.createConfig(ENVIRONMENT);
  }

  public static void disableLogging() throws NoSuchFieldException, IllegalAccessException {
    Log4jLoggerAdapter logger = (Log4jLoggerAdapter)LoggerFactory
      .getLogger(JobManager.class);
    Field loggerField = Log4jLoggerAdapter.class.getDeclaredField("logger");
    loggerField.setAccessible(true);
    Logger loggerObject = (Logger)loggerField.get(logger);


    Field repoField = Category.class.getDeclaredField("repository");
    repoField.setAccessible(true);
    LoggerRepository repoObject = (LoggerRepository)repoField.get(loggerObject);

    repoObject.setThreshold(Level.OFF);
  }

  public static void main(String[] args) throws Exception {

    //disableLogging();
    final String edges_global_file = "/Volumes/Untitled/Data/gMarkGen/OSN/100.txt";
    final String vertices_global_file = null;
    final String subgraphs = "/Volumes/Untitled/Data/gMarkGen/operands/100/";
    final String flattenedOutput = "/Users/vasistas/test";

    GraphCollectionDelta start = extractGraphFromFiles(edges_global_file, vertices_global_file,
      true);

    // Loading the graphs appearing in the graph collection
    // 1. Returning each vertex and edge files association
    File[] filesInDirectory = new File(subgraphs).listFiles();
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
          GraphCollectionDelta s = deltaUpdateGraphCollection(start, edgeFile, vertexFile, false);
          start = s;
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

    writeFlattenedGraph(heads, vertices, edges, flattenedOutput);

    DataSet<GraphHead> leftOperandHeads = start.getHeads()
      .filter(new Value1Of3AsFilter())
      .map(new Value2Of3<>());

    DataSet<Vertex> leftOperandVertices = vertices
      .joinWithTiny(leftOperandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    DataSet<Edge> leftOperandEdges = edges
      .joinWithTiny(leftOperandHeads)
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new SelectElementsInHeads<>());

    NestingIndex leftIndex = NestingBase.createIndex(LogicalGraph.fromDataSets(leftOperandHeads,
      leftOperandVertices,
      leftOperandEdges, CONFIGURATION));
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

    GraphCollection lg = GraphCollection.fromDataSets(heads, flattenedVertices, edges, CONFIGURATION);
    writeGraphCollection(lg, path);
    System.out.println(ExecutionEnvironment.getExecutionEnvironment().getExecutionPlan());
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
   * @param isLeftOperand         If the element is part of the left operand
   * @return                      Updates the graph definition
   */
  private static GraphCollectionDelta deltaUpdateGraphCollection(GraphCollectionDelta delta,
    String edges_global_file, String vertices_global_file, boolean isLeftOperand) {

    GraphCollectionDelta deltaPlus = extractGraphFromFiles(edges_global_file,
      vertices_global_file, isLeftOperand);

    return new GraphCollectionDelta(delta.getHeads().union(deltaPlus.getHeads()),
      delta.getVertices().union(deltaPlus.getVertices()),
      delta.getEdges().union(deltaPlus.getEdges()));
  }


}
