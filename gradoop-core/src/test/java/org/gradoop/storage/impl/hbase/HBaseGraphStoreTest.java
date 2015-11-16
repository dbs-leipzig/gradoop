package org.gradoop.storage.impl.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.HBaseTestBase;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;
import org.gradoop.storage.api.EPGMStore;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.PersistentVertexFactory;
import org.gradoop.storage.exceptions.UnsupportedTypeException;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class HBaseGraphStoreTest extends HBaseTestBase {

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * closes the store, opens it and reads/validates the data again.
   */
  @Test
  public void writeCloseOpenReadTest() {
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore =
      createEmptyEPGMStore();

    for (PersistentVertex<EdgePojo> v : createPersistentVertex()) {
      graphStore.writeVertex(v);
    }
    for (PersistentEdge<VertexPojo> e : createPersistentEdge()) {
      graphStore.writeEdge(e);
    }
    for (PersistentGraphHead g : createPersistentGraphHead()) {
      graphStore.writeGraphHead(g);
    }

    // re-open
    graphStore.close();
    graphStore = openEPGMStore();

    // validate
    validateGraphHead(graphStore);
    validateVertex(graphStore);
    validateEdge(graphStore);
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

    // store some data
    for (PersistentGraphHead g : createPersistentGraphHead()) {
      graphStore.writeGraphHead(g);
    }
    for (PersistentVertex<EdgePojo> v : createPersistentVertex()) {
      graphStore.writeVertex(v);
    }
    for (PersistentEdge<VertexPojo> e : createPersistentEdge()) {
      graphStore.writeEdge(e);
    }

    // flush changes
    graphStore.flush();

    // validate
    validateGraphHead(graphStore);
    validateVertex(graphStore);
    validateEdge(graphStore);

    graphStore.close();
  }

  /**
   * Stores some data and iterates over it. Checks correct amount.
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

    Collection<PersistentVertex<EdgePojo>> persistentVertexData =
      createPersistentSocialVertices();
    Collection<PersistentEdge<VertexPojo>> persistentEdgeData =
      createPersistentSocialEdges();
    Collection<PersistentGraphHead> persistentGraphData =
      createPersistentSocialGraphHead();

    // store some data
    for (PersistentGraphHead g : persistentGraphData) {
      graphStore.writeGraphHead(g);
    }
    for (PersistentVertex<EdgePojo> v : persistentVertexData) {
      graphStore.writeVertex(v);
    }
    for (PersistentEdge<VertexPojo> e : persistentEdgeData) {
      graphStore.writeEdge(e);
    }

    graphStore.flush();

    // check graph count
    int cnt = 0;
    for (Iterator<GraphHeadPojo> graphDataIterator =
         graphStore.getGraphSpace(); graphDataIterator.hasNext(); ) {
      cnt++;
    }
    assertEquals("wrong graph count", persistentGraphData.size(), cnt);

    // check vertex count
    cnt = 0;
    for (Iterator<VertexPojo> vertexDataIterator =
         graphStore.getVertexSpace(); vertexDataIterator.hasNext(); ) {
      cnt++;
    }
    assertEquals("wrong vertex count", persistentVertexData.size(), cnt);

    // check edge count
    cnt = 0;
    for (Iterator<EdgePojo> edgeDataIterator =
         graphStore.getEdgeSpace(); edgeDataIterator.hasNext(); ) {
      cnt++;
    }
    assertEquals("wrong edge count", persistentEdgeData.size(), cnt);

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

    GradoopId vertexID = GradoopId.fromLong(0L);
    final String label = "A";
    final Map<String, Object> properties = new HashMap<>();
    properties.put("k1", value);

    final Set<EdgePojo> outEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<EdgePojo> inEdges = Sets.newHashSetWithExpectedSize(0);
    final GradoopIds graphs = new GradoopIds();
    PersistentVertex<EdgePojo> v = persistentVertexFactory
      .createVertex(
        vertexFactory.createVertex(vertexID, label, properties, graphs),
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

    final int propertyCount = 6;
    final String keyBoolean = "key1";
    final boolean valueBoolean = true;
    final String keyInteger = "key2";
    final int valueInteger = 23;
    final String keyLong = "key3";
    final long valueLong = 42L;
    final String keyFloat = "key4";
    final float valueFloat = 13.37f;
    final String keyDouble = "key5";
    final double valueDouble = 3.14d;
    final String keyString = "key6";
    final String valueString = "value";

    final GradoopId vertexID = GradoopId.fromLong(0L);
    final String label = "A";

    final Map<String, Object> properties = new HashMap<>();
    properties.put(keyBoolean, valueBoolean);
    properties.put(keyInteger, valueInteger);
    properties.put(keyLong, valueLong);
    properties.put(keyFloat, valueFloat);
    properties.put(keyDouble, valueDouble);
    properties.put(keyString, valueString);

    final Set<EdgePojo> outEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<EdgePojo> inEdges = Sets.newHashSetWithExpectedSize(0);
    final GradoopIds graphs = new GradoopIds();

    // write to store
    graphStore.writeVertex(persistentVertexFactory.createVertex(
      vertexFactory.createVertex(vertexID, label, properties, graphs), outEdges,
      inEdges));

    graphStore.flush();

    // read from store
    EPGMVertex v = graphStore.readVertex(vertexID);
    List<String> propertyKeys = Lists.newArrayList(v.getPropertyKeys());
    assertEquals(propertyCount, propertyKeys.size());

    for (String propertyKey : propertyKeys) {
      switch (propertyKey) {
      case keyBoolean:
        assertEquals(valueBoolean, v.getProperty(propertyKey));
        break;
      case keyInteger:
        assertEquals(valueInteger, v.getProperty(keyInteger));
        break;
      case keyLong:
        assertEquals(valueLong, v.getProperty(keyLong));
        break;
      case keyFloat:
        assertEquals(valueFloat, v.getProperty(keyFloat));
        break;
      case keyDouble:
        assertEquals(valueDouble, v.getProperty(keyDouble));
        break;
      case keyString:
        assertEquals(valueString, v.getProperty(keyString));
        break;
      }
    }
  }
}