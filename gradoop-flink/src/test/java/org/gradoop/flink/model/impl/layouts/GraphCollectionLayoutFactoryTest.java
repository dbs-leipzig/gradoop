package org.gradoop.flink.model.impl.layouts;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.junit.Assert.assertEquals;

public abstract class GraphCollectionLayoutFactoryTest extends GradoopFlinkTestBase {

  protected abstract GraphCollectionLayoutFactory getFactory();

  @Test
  public void testFromDataSets() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    Collection<GraphHead> graphHeads = loader.getGraphHeads();
    Collection<Vertex> vertices = loader.getVertices();
    Collection<Edge> edges = loader.getEdges();

    DataSet<GraphHead> graphHeadDataSet = getExecutionEnvironment()
      .fromCollection(graphHeads);
    DataSet<Vertex> vertexDataSet = getExecutionEnvironment()
      .fromCollection(vertices);
    DataSet<Edge> edgeDataSet = getExecutionEnvironment()
      .fromCollection(edges);

    GraphCollectionLayout collectionLayout = getFactory()
      .fromDataSets(graphHeadDataSet, vertexDataSet, edgeDataSet);

    Collection<GraphHead> loadedGraphHeads  = Lists.newArrayList();
    Collection<Vertex> loadedVertices       = Lists.newArrayList();
    Collection<Edge> loadedEdges            = Lists.newArrayList();

    collectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(edges, loadedEdges);
  }

  @Test
  public void testFromDataSetsWithoutGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("()-[]->(),[()]");

    GraphCollectionLayout collectionLayout = getFactory()
      .fromDataSets(
        getExecutionEnvironment().fromCollection(loader.getGraphHeads()),
        getExecutionEnvironment().fromCollection(loader.getVertices()),
        getExecutionEnvironment().fromCollection(loader.getEdges()));

    Collection<GraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<Vertex> loadedVertices   = Lists.newArrayList();
    Collection<Edge> loadedEdges      = Lists.newArrayList();

    collectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(loader.getGraphHeads(), loadedGraphHeads);
    validateEPGMElementCollections(loader.getVertices(), loadedVertices);
    validateEPGMElementCollections(loader.getEdges(), loadedEdges);
    validateEPGMGraphElementCollections(loader.getVertices(), loadedVertices);
    validateEPGMGraphElementCollections(loader.getEdges(), loadedEdges);
  }

  @Test
  public void testFromCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollectionLayout collectionLayout = getFactory()
      .fromCollections(loader.getGraphHeads(),
        loader.getVertices(),
        loader.getEdges());

    Collection<GraphHead> loadedGraphHeads  = Lists.newArrayList();
    Collection<Vertex> loadedVertices       = Lists.newArrayList();
    Collection<Edge> loadedEdges            = Lists.newArrayList();

    collectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(loader.getGraphHeads(), loadedGraphHeads);
    validateEPGMElementCollections(loader.getVertices(), loadedVertices);
    validateEPGMElementCollections(loader.getEdges(), loadedEdges);
    validateEPGMGraphElementCollections(loader.getVertices(), loadedVertices);
    validateEPGMGraphElementCollections(loader.getEdges(), loadedEdges);
  }

  @Test
  public void testFromGraphTransactions() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphTransaction g0 = new GraphTransaction(loader.getGraphHeadByVariable("g0"),
      Sets.newHashSet(loader.getVerticesByGraphVariables("g0")),
      Sets.newHashSet(loader.getEdgesByGraphVariables("g0")));

    GraphTransaction g1 = new GraphTransaction(loader.getGraphHeadByVariable("g1"),
      Sets.newHashSet(loader.getVerticesByGraphVariables("g1")),
      Sets.newHashSet(loader.getEdgesByGraphVariables("g1")));

    GraphTransaction g2 = new GraphTransaction(loader.getGraphHeadByVariable("g2"),
      Sets.newHashSet(loader.getVerticesByGraphVariables("g2")),
      Sets.newHashSet(loader.getEdgesByGraphVariables("g2")));

    DataSet<GraphTransaction> transactions = getExecutionEnvironment().fromElements(g0, g1, g2);

    GraphCollectionLayout collectionLayout = getFactory().fromTransactions(transactions);

    Collection<GraphHead> loadedGraphHeads  = Lists.newArrayList();
    Collection<Vertex> loadedVertices       = Lists.newArrayList();
    Collection<Edge> loadedEdges            = Lists.newArrayList();

    collectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(loader.getGraphHeadsByVariables("g0", "g1", "g2"), loadedGraphHeads);
    validateEPGMElementCollections(loader.getVerticesByGraphVariables("g0", "g1", "g2"), loadedVertices);
    validateEPGMElementCollections(loader.getEdgesByGraphVariables("g0", "g1", "g2"), loadedEdges);
    validateEPGMGraphElementCollections(loader.getVerticesByGraphVariables("g0", "g1", "g2"), loadedVertices);
    validateEPGMGraphElementCollections(loader.getEdgesByGraphVariables("g0", "g1", "g2"), loadedEdges);
  }

  @Test
  public void testCreateEmptyCollection() throws Exception {
    GraphCollectionLayout graphCollectionLayout = getFactory().createEmptyCollection();

    Collection<GraphHead> loadedGraphHeads  = Lists.newArrayList();
    Collection<Vertex> loadedVertices       = Lists.newArrayList();
    Collection<Edge> loadedEdges            = Lists.newArrayList();

    graphCollectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    graphCollectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    graphCollectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    assertEquals(0L, loadedGraphHeads.size());
    assertEquals(0L, loadedVertices.size());
    assertEquals(0L, loadedEdges.size());
  }
}
