package org.gradoop.flink.model.impl.layouts.gve;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.functions.utils.First;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * Responsible for creating a {@link GVELayout} from given data.
 */
public class GVECollectionLayoutFactory extends GVEBaseFactory implements GraphCollectionLayoutFactory {

  @Override
  public GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices) {
    return fromDataSets(graphHeads, vertices,
      createEdgeDataSet(new ArrayList<>(0)));
  }

  @Override
  public GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    Objects.requireNonNull(graphHeads, "GraphHead DataSet was null");
    Objects.requireNonNull(vertices, "Vertex DataSet was null");
    Objects.requireNonNull(edges, "Edge DataSet was null");
    Objects.requireNonNull(config, "Config was null");
    return new GVELayout(graphHeads, vertices, edges);
  }

  @Override
  public GraphCollectionLayout fromCollections(Collection<GraphHead> graphHeads,
    Collection<Vertex> vertices, Collection<Edge> edges) {
    Objects.requireNonNull(graphHeads, "GraphHead collection was null");
    Objects.requireNonNull(vertices, "Vertex collection was null");
    Objects.requireNonNull(edges, "Vertex collection was null");
    return fromDataSets(
      createGraphHeadDataSet(graphHeads),
      createVertexDataSet(vertices),
      createEdgeDataSet(edges));
  }

  @Override
  public GraphCollectionLayout fromGraphLayout(LogicalGraphLayout graph) {
    return fromDataSets(graph.getGraphHead(), graph.getVertices(), graph.getEdges());
  }

  @Override
  public GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions) {
    GroupReduceFunction<Vertex, Vertex> vertexReducer = new First<>();
    GroupReduceFunction<Edge, Edge> edgeReducer = new First<>();

    return fromTransactions(transactions, vertexReducer, edgeReducer);
  }

  @Override
  public GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer) {

    DataSet<Tuple3<GraphHead, Set<Vertex>, Set<Edge>>> triples = transactions
      .map(new GraphTransactionTriple());

    DataSet<GraphHead> graphHeads = triples.map(new TransactionGraphHead());

    DataSet<Vertex> vertices = triples
      .flatMap(new TransactionVertices())
      .groupBy(new Id<>())
      .reduceGroup(vertexMergeReducer);

    DataSet<Edge> edges = triples
      .flatMap(new TransactionEdges())
      .groupBy(new Id<>())
      .reduceGroup(edgeMergeReducer);

    return fromDataSets(graphHeads, vertices, edges);
  }

  @Override
  public GraphCollectionLayout createEmptyCollection() {
    Collection<GraphHead> graphHeads = new ArrayList<>();
    Collection<Vertex> vertices = new ArrayList<>();
    Collection<Edge> edges = new ArrayList<>();

    return fromCollections(graphHeads, vertices, edges);
  }
}
