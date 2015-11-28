/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.storage.impl.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;
import org.gradoop.model.api.EPGMProperties;
import org.gradoop.model.impl.properties.Properties;
import org.gradoop.storage.api.EPGMStore;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.PersistentVertexFactory;
import org.gradoop.storage.exceptions.UnsupportedTypeException;
import org.gradoop.util.AsciiGraphLoader;
import org.gradoop.util.GradoopConfig;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static org.gradoop.GradoopTestUtils.*;
import static org.gradoop.storage.impl.hbase.GradoopHBaseTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HBaseGraphStoreTest extends GradoopHBaseTestBase {

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * closes the store, opens it and reads/validates the data again.
   */
  @Test
  public void writeCloseOpenReadTest() {
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore =
      createEmptyEPGMStore();

    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getMinimalFullFeaturedGraphLoader();

    GraphHeadPojo graphHead = loader.getGraphHeads().iterator().next();
    VertexPojo vertex = loader.getVertices().iterator().next();
    EdgePojo edge = loader.getEdges().iterator().next();

    writeGraphHead(graphStore, graphHead, vertex, edge);
    writeVertex(graphStore, vertex, edge);
    writeEdge(graphStore, vertex, edge);

    // re-open
    graphStore.close();
    graphStore = openEPGMStore();

    // validate
    validateGraphHead(graphStore, graphHead);
    validateVertex(graphStore, vertex);
    validateEdge(graphStore, edge);
    graphStore.close();
  }

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * flushes the tables and reads/validates the data.
   */
  @Test
  public void writeFlushReadTest() {
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore =
      createEmptyEPGMStore();
    graphStore.setAutoFlush(false);

    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getMinimalFullFeaturedGraphLoader();

    GraphHeadPojo graphHead = loader.getGraphHeads().iterator().next();
    VertexPojo vertex = loader.getVertices().iterator().next();
    EdgePojo edge = loader.getEdges().iterator().next();

    writeGraphHead(graphStore, graphHead, vertex, edge);
    writeVertex(graphStore, vertex, edge);
    writeEdge(graphStore, vertex, edge);

    // flush changes
    graphStore.flush();

    // validate
    validateGraphHead(graphStore, graphHead);
    validateVertex(graphStore, vertex);
    validateEdge(graphStore, edge);

    graphStore.close();
  }

  /**
   * Stores social network data, loads it again and checks for element data
   * equality.
   *
   * @throws InterruptedException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void iteratorTest() throws InterruptedException, IOException,
    ClassNotFoundException {
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore =
      createEmptyEPGMStore();

    List<PersistentVertex<EdgePojo>> vertices =
      Lists.newArrayList(getSocialPersistentVertices());
    List<PersistentEdge<VertexPojo>> edges =
      Lists.newArrayList(getSocialPersistentEdges());
    List<PersistentGraphHead> graphHeads =
      Lists.newArrayList(getSocialPersistentGraphHeads());

    // store some data
    for (PersistentGraphHead g : graphHeads) {
      graphStore.writeGraphHead(g);
    }
    for (PersistentVertex<EdgePojo> v : vertices) {
      graphStore.writeVertex(v);
    }
    for (PersistentEdge<VertexPojo> e : edges) {
      graphStore.writeEdge(e);
    }

    graphStore.flush();

    // graph heads
    validateEPGMElementCollections(
      graphHeads,
      Lists.newArrayList(graphStore.getGraphSpace())
    );
    // vertices
    validateEPGMElementCollections(
      vertices,
      Lists.newArrayList(graphStore.getVertexSpace())
    );
    validateEPGMGraphElementCollections(
      vertices,
      Lists.newArrayList(graphStore.getVertexSpace())
    );
    // edges
    validateEPGMElementCollections(
      edges,
      Lists.newArrayList(graphStore.getEdgeSpace())
    );
    validateEPGMGraphElementCollections(
      edges,
      Lists.newArrayList(graphStore.getEdgeSpace())
    );

    graphStore.close();
  }

  /**
   * Tries to add an unsupported property type {@link List} as property value.
   */
  @Test(expected = UnsupportedTypeException.class)
  public void wrongPropertyTypeTest() {
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore =
      createEmptyEPGMStore();

    PersistentVertexFactory<VertexPojo, EdgePojo, HBaseVertex>
      persistentVertexFactory = new HBaseVertexFactory();
    EPGMVertexFactory<VertexPojo> vertexFactory =
      new VertexPojoFactory();

    // list is not supported by
    final List<String> value = Lists.newArrayList();

    GradoopId vertexID = GradoopId.get();
    final String label = "A";
    EPGMProperties props = new Properties();
    props.set("k1", value);

    final Set<EdgePojo> outEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<EdgePojo> inEdges = Sets.newHashSetWithExpectedSize(0);
    final GradoopIdSet graphs = new GradoopIdSet();
    PersistentVertex<EdgePojo> v = persistentVertexFactory
      .createVertex(
        vertexFactory.initVertex(vertexID, label, props, graphs),
        outEdges, inEdges);

    graphStore.writeVertex(v);
  }

  /**
   * Checks if property values are read correctly.
   */
  @Test
  public void propertyTypeTest() {
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore =
      createEmptyEPGMStore();

    PersistentVertexFactory<VertexPojo, EdgePojo, HBaseVertex>
      persistentVertexFactory = new HBaseVertexFactory();
    EPGMVertexFactory<VertexPojo> vertexFactory =
      new VertexPojoFactory();

    final int propertyCount = 7;
    final String keyBoolean = "key1";
    final Boolean valueBoolean = true;
    final String keyInteger = "key2";
    final Integer valueInteger = 23;
    final String keyLong = "key3";
    final Long valueLong = 42L;
    final String keyFloat = "key4";
    final Float valueFloat = 13.37f;
    final String keyDouble = "key5";
    final Double valueDouble = 3.14d;
    final String keyString = "key6";
    final String valueString = "value";
    final String keyBigDecimal = "key7";
    final BigDecimal valueBigDecimal = new BigDecimal(10L);

    // TODO: fails because of wrong ISOCHRONOLOGY after serializing
//    final String keyDateTime = "key8";
//    final DateTime valueDateTime = new DateTime();

    final GradoopId vertexID = GradoopId.get();
    final String label = "A";

    EPGMProperties props = new Properties();
    props.set(keyBoolean, valueBoolean);
    props.set(keyInteger, valueInteger);
    props.set(keyLong, valueLong);
    props.set(keyFloat, valueFloat);
    props.set(keyDouble, valueDouble);
    props.set(keyString, valueString);
    props.set(keyBigDecimal, valueBigDecimal);
//    props.set(keyDateTime, valueDateTime);

    final Set<EdgePojo> outEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<EdgePojo> inEdges = Sets.newHashSetWithExpectedSize(0);
    final GradoopIdSet graphs = new GradoopIdSet();

    // write to store
    graphStore.writeVertex(persistentVertexFactory.createVertex(
      vertexFactory.initVertex(vertexID, label, props, graphs), outEdges,
      inEdges));

    graphStore.flush();

    // read from store
    EPGMVertex v = graphStore.readVertex(vertexID);
    List<String> propertyKeys = Lists.newArrayList(v.getPropertyKeys());
    assertEquals(propertyCount, propertyKeys.size());

    for (String propertyKey : propertyKeys) {
      switch (propertyKey) {
      case keyBoolean:
        assertTrue(v.getPropertyValue(propertyKey).isBoolean());
        assertEquals(valueBoolean, v.getPropertyValue(propertyKey).getBoolean());
        break;
      case keyInteger:
        assertTrue(v.getPropertyValue(propertyKey).isInt());
        assertEquals((int) valueInteger, v.getPropertyValue(propertyKey).getInt());
        break;
      case keyLong:
        assertTrue(v.getPropertyValue(propertyKey).isLong());
        assertEquals((long) valueLong, v.getPropertyValue(propertyKey).getLong());
        break;
      case keyFloat:
        assertTrue(v.getPropertyValue(propertyKey).isFloat());
        assertEquals(valueFloat, v.getPropertyValue(propertyKey).getFloat(), 0);
        break;
      case keyDouble:
        assertTrue(v.getPropertyValue(propertyKey).isDouble());
        assertEquals(valueDouble, v.getPropertyValue(propertyKey).getDouble(), 0);
        break;
      case keyString:
        assertTrue(v.getPropertyValue(propertyKey).isString());
        assertEquals(valueString, v.getPropertyValue(propertyKey).getString());
        break;
      case keyBigDecimal:
        assertTrue(v.getPropertyValue(propertyKey).isBigDecimal());
        assertEquals(valueBigDecimal, v.getPropertyValue(propertyKey).getBigDecimal());
        break;
//      case keyDateTime:
//        assertTrue(v.getPropertyValue(propertyKey).isDateTime());
//        assertEquals(valueDateTime, v.getPropertyValue(propertyKey).getDateTime());
//        break;
      }
    }
  }

  private AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
    getMinimalFullFeaturedGraphLoader() {
    String asciiGraph = ":G{k=\"v\"}[(v:V{k=\"v\"});(v)-[:e{k=\"v\"}]->(v)]";

    return AsciiGraphLoader.fromString(
      asciiGraph, GradoopConfig.getDefaultConfig());
  }

  private void writeGraphHead(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore,
    GraphHeadPojo graphHead, VertexPojo vertex, EdgePojo edge) {
    graphStore.writeGraphHead(
      new HBaseGraphHeadFactory().createGraphHead(
        graphHead,
        GradoopIdSet.fromExisting(vertex.getId()),
        GradoopIdSet.fromExisting(edge.getId())
      )
    );
  }

  private void writeVertex(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore,
    VertexPojo vertex, EdgePojo edge) {
    graphStore.writeVertex(
      new HBaseVertexFactory().createVertex(
        vertex,
        Sets.newHashSet(edge),
        Sets.newHashSet(edge)
      )
    );
  }

  private void writeEdge(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore,
    VertexPojo vertex, EdgePojo edge) {

    graphStore.writeEdge(
      new HBaseEdgeFactory()
        .createEdge(
          edge,
          vertex,
          vertex
        )
    );
  }

  private void validateGraphHead(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore,
    GraphHeadPojo originalGraphHead) {

    EPGMGraphHead loadedGraphHead = graphStore
      .readGraph(originalGraphHead.getId());

    validateEPGMElements(originalGraphHead, loadedGraphHead);
  }

  private void validateVertex(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore,
    VertexPojo originalVertex) {

    EPGMVertex loadedVertex = graphStore.readVertex(originalVertex.getId());

    validateEPGMElements(originalVertex, loadedVertex);
    validateEPGMGraphElements(originalVertex, loadedVertex);
  }

  private void validateEdge(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore,
    EdgePojo originalEdge) {

    EPGMEdge loadedEdge = graphStore.readEdge(originalEdge.getId());

    validateEPGMElements(originalEdge, loadedEdge);
    validateEPGMGraphElements(originalEdge, loadedEdge);

    assertTrue(
      "source vertex mismatch",
      originalEdge.getSourceId().equals(
        loadedEdge.getSourceId())
    );

    assertTrue(
      "target vertex mismatch",
      originalEdge.getTargetId().equals(
        loadedEdge.getTargetId()
      )
    );
  }

}