package org.gradoop.flink.model.impl.epgm;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalElement;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.functions.timeextractors.TimeIntervalExtractor;
import org.gradoop.flink.model.api.functions.timeextractors.TimestampExtractor;
import org.gradoop.flink.model.api.tpgm.TemporalGraphCollection;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link GraphCollection}
 */
public class GraphCollectionTest extends GradoopFlinkTestBase {
  /**
   * Test the {@link GraphCollection#toTemporalGraph()} function.
   *
   * @throws Exception if the test execution fails
   */
  @Test
  public void testToTemporalGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection graphCollection = loader.getGraphCollection();

    // Call the function to test
    TemporalGraphCollection temporalGraphCollection = graphCollection.toTemporalGraph();

    // use collections as data sink
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();
    Collection<GraphHead> graphHeads = Lists.newArrayList();

    Collection<TemporalVertex> temporalVertices = Lists.newArrayList();
    Collection<TemporalEdge> temporalEdges = Lists.newArrayList();
    Collection<TemporalGraphHead> temporalGraphHeads = Lists.newArrayList();

    graphCollection.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    graphCollection.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    graphCollection.getGraphHeads().output(new LocalCollectionOutputFormat<>(graphHeads));

    temporalGraphCollection.getVertices().output(
      new LocalCollectionOutputFormat<>(temporalVertices));
    temporalGraphCollection.getEdges().output(
      new LocalCollectionOutputFormat<>(temporalEdges));
    temporalGraphCollection.getGraphHeads().output(
      new LocalCollectionOutputFormat<>(temporalGraphHeads));

    getExecutionEnvironment().execute();

    assertEquals(7, vertices.size());
    assertEquals(13, edges.size());
    assertEquals(4, graphHeads.size());

    assertEquals(7, temporalVertices.size());
    assertEquals(13, temporalEdges.size());
    assertEquals(4, temporalGraphHeads.size());

    validateEPGMElementCollections(vertices, temporalVertices);
    validateEPGMElementCollections(edges, temporalEdges);
    validateEPGMElementCollections(graphHeads, temporalGraphHeads);

    temporalVertices.forEach(this::checkDefaultTemporalElement);
    temporalEdges.forEach(this::checkDefaultTemporalElement);
    temporalGraphHeads.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link GraphCollection#toTemporalGraph(TimestampExtractor, TimestampExtractor,
   * TimestampExtractor)} function.
   *
   * @throws Exception if the test execution fails
   */
  @Test
  public void testToTemporalGraphWithTimestampExtractor() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    GraphCollection graphCollection = loader.getGraphCollection();
    Long validFrom = 42L;

    // Call the function to test
    TemporalGraphCollection temporalGraphCollection = graphCollection
      .toTemporalGraph(gh -> validFrom, v -> validFrom, e -> validFrom);

    // use collections as data sink
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();
    Collection<GraphHead> graphHeads = Lists.newArrayList();

    Collection<TemporalVertex> temporalVertices = Lists.newArrayList();
    Collection<TemporalEdge> temporalEdges = Lists.newArrayList();
    Collection<TemporalGraphHead> temporalGraphHeads = Lists.newArrayList();

    graphCollection.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    graphCollection.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    graphCollection.getGraphHeads().output(new LocalCollectionOutputFormat<>(graphHeads));

    temporalGraphCollection.getVertices()
      .output(new LocalCollectionOutputFormat<>(temporalVertices));
    temporalGraphCollection.getEdges()
      .output(new LocalCollectionOutputFormat<>(temporalEdges));
    temporalGraphCollection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(temporalGraphHeads));

    getExecutionEnvironment().execute();

    assertEquals(7, vertices.size());
    assertEquals(13, edges.size());
    assertEquals(4, graphHeads.size());

    assertEquals(7, temporalVertices.size());
    assertEquals(13, temporalEdges.size());
    assertEquals(4, temporalGraphHeads.size());

    validateEPGMElementCollections(vertices, temporalVertices);
    validateEPGMElementCollections(edges, temporalEdges);
    validateEPGMElementCollections(graphHeads, temporalGraphHeads);

    // Check if there are default transaction times set
    temporalVertices.forEach(this::checkDefaultTxTimes);
    temporalEdges.forEach(this::checkDefaultTxTimes);
    temporalGraphHeads.forEach(this::checkDefaultTxTimes);

    // Check if the validFrom values are equal with the expected ones
    temporalGraphHeads.forEach(tg -> assertEquals(validFrom, tg.getValidFrom()));
    temporalVertices.forEach(tv -> assertEquals(validFrom, tv.getValidFrom()));
    temporalEdges.forEach(te -> assertEquals(validFrom, te.getValidFrom()));

    // Check if the validTo values are equal with the default ones
    temporalGraphHeads
      .forEach(tg -> assertEquals(TemporalElement.DEFAULT_VALID_TIME, tg.getValidTo()));
    temporalVertices
      .forEach(tv -> assertEquals(TemporalElement.DEFAULT_VALID_TIME, tv.getValidTo()));
    temporalEdges
      .forEach(te -> assertEquals(TemporalElement.DEFAULT_VALID_TIME, te.getValidTo()));
  }

  /**
   * Test the {@link GraphCollection#toTemporalGraph(TimeIntervalExtractor, TimeIntervalExtractor,
   * TimeIntervalExtractor)}  function.
   *
   * @throws Exception if the test execution fails
   */
  @Test
  public void testToTemporalGraphWithTimeIntervalExtractor() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    GraphCollection graphCollection = loader.getGraphCollection();

    // Call the function to test
    TemporalGraphCollection temporalGraphCollection = graphCollection.toTemporalGraph(
      GradoopFlinkTestUtils.getGraphHeadTimeIntervalExtractor(),
      GradoopFlinkTestUtils.getVertexTimeIntervalExtractor(),
      GradoopFlinkTestUtils.getEdgeTimeIntervalExtractor());

    // use collections as data sink
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();
    Collection<GraphHead> graphHeads = Lists.newArrayList();

    Collection<TemporalVertex> temporalVertices = Lists.newArrayList();
    Collection<TemporalEdge> temporalEdges = Lists.newArrayList();
    Collection<TemporalGraphHead> temporalGraphHeads = Lists.newArrayList();

    graphCollection.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    graphCollection.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    graphCollection.getGraphHeads().output(new LocalCollectionOutputFormat<>(graphHeads));

    temporalGraphCollection.getVertices().output(
      new LocalCollectionOutputFormat<>(temporalVertices));
    temporalGraphCollection.getEdges().output(
      new LocalCollectionOutputFormat<>(temporalEdges));
    temporalGraphCollection.getGraphHeads().output(
      new LocalCollectionOutputFormat<>(temporalGraphHeads));

    getExecutionEnvironment().execute();

    assertEquals(7, vertices.size());
    assertEquals(13, edges.size());
    assertEquals(4, graphHeads.size());

    assertEquals(7, temporalVertices.size());
    assertEquals(13, temporalEdges.size());
    assertEquals(4, temporalGraphHeads.size());

    validateEPGMElementCollections(vertices, temporalVertices);
    validateEPGMElementCollections(edges, temporalEdges);
    validateEPGMElementCollections(graphHeads, temporalGraphHeads);

    // Check if there are default transaction times set
    temporalVertices.forEach(this::checkDefaultTxTimes);
    temporalEdges.forEach(this::checkDefaultTxTimes);
    temporalGraphHeads.forEach(this::checkDefaultTxTimes);

    // Check if the validFrom values are equal with the expected ones
    temporalGraphHeads.forEach(tg -> {
      assertEquals((Long) 42L, tg.getValidFrom());
      assertEquals((Long) 52L, tg.getValidTo());
    });
    temporalVertices.forEach(tv -> {
      assertEquals((Long) 52L, tv.getValidFrom());
      assertEquals((Long) 62L, tv.getValidTo());
    });
    temporalEdges.forEach(te -> {
      assertEquals((Long) 62L, te.getValidFrom());
      assertEquals((Long) 72L, te.getValidTo());
    });
  }
}
