package org.gradoop.benchmark.nesting;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.benchmark.nesting.functions.*;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;
import org.gradoop.flink.util.GradoopFlinkConfig;

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

  public static void main(String[] args) throws Exception {

    final String edges_global_file = "/Volumes/Untitled/Data/gMarkGen/OSN/100.txt";
    final String vertices_global_file = null;

    GraphCollectionDelta s = extractGraphFromFiles(edges_global_file, vertices_global_file);
    DataSet<Tuple2<String, Vertex>> intermediateVertex = createGraphVertices(s.getVertices());

    DataSet<GraphHead> heads = s.getHeads().map(new Value2Of3<>());

    DataSet<Edge> edges = s.getEdges()
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

    GraphCollection lg = GraphCollection.fromDataSets(heads, vertices, edges, CONFIGURATION);
    writeGraphCollection(lg, "/Users/vasistas/test");

  }

  private static DataSet<Tuple2<String, Vertex>> createGraphVertices
    (DataSet<Tuple2<ImportVertex<String>, GradoopId>> vertex) {
    return vertex
      .groupBy(new SelectImportVertexId())
      .combineGroup(new CreateVertex<>(CONFIGURATION.getVertexFactory()));
  }

  private static GraphCollectionDelta extractGraphFromFiles(String edges_global_file, String vertices_global_file) {
    // This information is going to be used when serializing the operands
    DataSet<Tuple3<String, Boolean, GraphHead>> heads =
      ENVIRONMENT.fromElements(edges_global_file)
        .map(new AssociateFileToGraph(true, CONFIGURATION.getGraphHeadFactory()));

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

  


}
