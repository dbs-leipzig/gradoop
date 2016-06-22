package org.gradoop.io.impl.tsv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.tsv.functions.TSVToTuple;
import org.gradoop.io.impl.tsv.functions.TupleReducer;
import org.gradoop.io.impl.tsv.functions.VertexReducer;
import org.gradoop.io.impl.tsv.functions.TupleToEdge;
import org.gradoop.io.impl.tsv.functions.TupleToVertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.combination.ReduceCombination;
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
    return getGraphCollection().reduce(new ReduceCombination<G, V, E>());
  }

  @Override
  public GraphCollection<G, V, E> getGraphCollection() throws IOException {

    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    //--------------------------------------------------------------------------
    // create type information
    //--------------------------------------------------------------------------

    // used for type hinting when loading vertex data
    TypeInformation<V> vertexTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<E> edgeTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getEdgeFactory().getType());

    //--------------------------------------------------------------------------
    // create tuple that contains all information
    //--------------------------------------------------------------------------

    DataSet<Tuple6<String, GradoopId, String, String, GradoopId, String>>
      lineTupel = env.readTextFile(getTsvPath()).map(new TSVToTuple());

    // reduces duplicates in source and target ids
    lineTupel = lineTupel.reduceGroup(new TupleReducer());

    //--------------------------------------------------------------------------
    // compute edges
    //--------------------------------------------------------------------------

    DataSet<E> edges = lineTupel
      .map(new TupleToEdge<>(getConfig().getEdgeFactory()))
      .returns(edgeTypeInfo);

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    DataSet<V> vertices = lineTupel
      .flatMap(new TupleToVertex<>(getConfig().getVertexFactory()))
      .returns(vertexTypeInfo);

    // remove vertex duplicates
    vertices = vertices.reduceGroup(new VertexReducer<V>());

    //--------------------------------------------------------------------------
    // generate new graphs
    //--------------------------------------------------------------------------

    DataSet<G> graphHeads =  env.fromElements(
      getConfig().getGraphHeadFactory().createGraphHead());

    return GraphCollection.fromDataSets(graphHeads, vertices, edges,
      getConfig());
  }

  @Override
  public GraphTransactions<G, V, E> getGraphTransactions() throws IOException {
    return getGraphCollection().toTransactions();
  }
}
