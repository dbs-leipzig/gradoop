package org.gradoop.flink.io.impl.minimal;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class MinimalDataSource implements DataSource {

  /**
   * Path to the vertex csv file
   */
  private String vertexCsvPath;

  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * Token delimiter
   */
  private static String tokenSeparator;

  /**
   * Name of the id column
   */
  private String vertexIdColumn;

  /**
   * Name of the label column
   */
  private String vertexLabel;

  /**
   * Property names of all vertices
   */
  private static List<String> vertexProperties;
  
  /**
   * Path to the edge csv file
   */
  private String edgeCsvPath;
  
  /**
   * Name of the edge id column
   */
  private String edgeIdColumn;

  /**
   * Name of the source id column
   */
  private String sourceIdColumn;

  /**
   * Name of the target id column
   */
  private String targetIdColumn;

  /**
   * Name of the label column
   */
  private String edgeLabel;

  /**
   * Property names of all vertices
   */
  private static List<String> edgeProperties;
  
  public MinimalDataSource(GradoopFlinkConfig config, String tokenSeperator,
      String vertexCsvPath, String vertexIdColumn, String vertexLabel,
      List<String> vertexProperties, String edgeCsvPath, String edgeId,
      String sourceId, String targetId, String edgeLabel, List<String> edgeProperties) {
    this.config = config;
    this.tokenSeparator = tokenSeperator;
    this.vertexCsvPath = vertexCsvPath;
    this.vertexIdColumn = vertexIdColumn;
    this.vertexLabel = vertexLabel;
    this.vertexProperties = vertexProperties;
    this.edgeCsvPath = edgeCsvPath;
    this.edgeIdColumn = edgeId;
    this.sourceIdColumn = sourceId;
    this.targetIdColumn = targetId;
    this.edgeLabel = edgeLabel;
    this.edgeProperties = edgeProperties;
  }
  
  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    
    MinimalVertexProvider verticesProvide = new MinimalVertexProvider(
        vertexCsvPath, config, tokenSeparator, vertexIdColumn, vertexLabel, vertexProperties);
    
    DataSet<ImportVertex<String>> importVertices = verticesProvide.importVertex();
    
    MinimalEdgeProvider edgesProvider = new MinimalEdgeProvider(
        edgeCsvPath, config, tokenSeparator, edgeIdColumn, sourceIdColumn,
        targetIdColumn, edgeLabel, edgeProperties);
    
    DataSet<ImportEdge<String>> importEdges = edgesProvider.importEdge();
    
    return new GraphDataSource<>(importVertices, importEdges, getConfig()).getLogicalGraph();
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  
  GradoopFlinkConfig getConfig() {
    return config;
  }
}