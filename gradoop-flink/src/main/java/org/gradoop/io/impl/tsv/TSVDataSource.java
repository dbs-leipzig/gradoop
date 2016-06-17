package org.gradoop.io.impl.tsv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.tsv.functions.TSVToEdge;
import org.gradoop.io.impl.tsv.functions.TSVToVertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Class to create a LogicalGraph from TSV-Input source
 */
public class TSVDataSource
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends TSVBase<G,V,E>
  implements DataSource<G,V,E> {

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param tsvPath       Path to TSV-File
   * @param config        Gradoop Flink configuration
   */
  public TSVDataSource(String tsvPath, GradoopFlinkConfig<G, V, E> config) {
    super(tsvPath, config);
  }


  @Override
  public LogicalGraph<G, V, E> getLogicalGraph() throws IOException {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    // used for type hinting when loading vertex data
    TypeInformation<V> vertexTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<E> edgeTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getEdgeFactory().getType());


//    TSVToGraph<V,E> gen = new TSVToGraph<>(getTsvPath(),
//      getConfig().getVertexFactory(),
//      getConfig().getEdgeFactory());
//
//    DataSet<V> vertices = gen.getVertices();
//
//    DataSet<E> edges = gen.getEdges();


    // read vertex, edge and graph data
    DataSet<V> vertices = env.readTextFile(getTsvPath())
      .map(new TSVToVertex<>(getConfig().getVertexFactory()))
      .returns(vertexTypeInfo);

    DataSet<E> edges = env.readTextFile(getTsvPath())
      .map(new TSVToEdge<>(getConfig().getEdgeFactory())).returns
        (edgeTypeInfo);

    DataSet<G> graphHeads =  env.fromElements(
      getConfig().getGraphHeadFactory().createGraphHead());



    return LogicalGraph.fromDataSets(graphHeads, vertices, edges, getConfig());
  }

  @Override
  public GraphCollection<G, V, E> getGraphCollection() throws IOException {
    return null;
  }

  @Override
  public GraphTransactions<G, V, E> getGraphTransactions() throws IOException {
    return null;
  }
}
