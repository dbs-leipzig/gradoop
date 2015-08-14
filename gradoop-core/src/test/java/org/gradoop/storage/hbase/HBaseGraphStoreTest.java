package org.gradoop.storage.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.HBaseTest;
import org.gradoop.model.VertexData;
import org.gradoop.model.VertexDataFactory;
import org.gradoop.model.impl.DefaultEdgeData;
import org.gradoop.model.impl.DefaultGraphData;
import org.gradoop.model.impl.DefaultVertexData;
import org.gradoop.model.impl.DefaultVertexDataFactory;
import org.gradoop.storage.EPGMStore;
import org.gradoop.storage.PersistentEdgeData;
import org.gradoop.storage.PersistentGraphData;
import org.gradoop.storage.PersistentVertexData;
import org.gradoop.storage.PersistentVertexDataFactory;
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

public class HBaseGraphStoreTest extends HBaseTest {

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * closes the store, opens it and reads/validates the data again.
   */
  @Test
  public void writeCloseOpenReadTest() {
    EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData> graphStore =
      createEmptyEPGMStore();

    for (PersistentVertexData<DefaultEdgeData> v : createPersistentVertexData
      ()) {
      graphStore.writeVertexData(v);
    }
    for (PersistentEdgeData<DefaultVertexData> e : createPersistentEdgeData()) {
      graphStore.writeEdgeData(e);
    }
    for (PersistentGraphData g : createPersistentGraphData()) {
      graphStore.writeGraphData(g);
    }

    // re-open
    graphStore.close();
    graphStore = openEPGMStore();

    // validate
    validateGraphData(graphStore);
    validateVertexData(graphStore);
    validateEdgeData(graphStore);
    graphStore.close();
  }

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * flushes the tables and reads/validates the data.
   */
  @Test
  public void writeFlushReadTest() {
    EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData> graphStore =
      createEmptyEPGMStore();
    graphStore.setAutoFlush(false);

    // store some data
    for (PersistentGraphData g : createPersistentGraphData()) {
      graphStore.writeGraphData(g);
    }
    for (PersistentVertexData<DefaultEdgeData> v : createPersistentVertexData
      ()) {
      graphStore.writeVertexData(v);
    }
    for (PersistentEdgeData<DefaultVertexData> e : createPersistentEdgeData()) {
      graphStore.writeEdgeData(e);
    }

    // flush changes
    graphStore.flush();

    // validate
    validateGraphData(graphStore);
    validateVertexData(graphStore);
    validateEdgeData(graphStore);

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
    EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData> graphStore =
      createEmptyEPGMStore();

    Collection<PersistentVertexData<DefaultEdgeData>> persistentVertexData =
      createPersistentSocialVertexData();
    Collection<PersistentEdgeData<DefaultVertexData>> persistentEdgeData =
      createPersistentSocialEdgeData();
    Collection<PersistentGraphData> persistentGraphData =
      createPersistentSocialGraphData();

    // store some data
    for (PersistentGraphData g : persistentGraphData) {
      graphStore.writeGraphData(g);
    }
    for (PersistentVertexData<DefaultEdgeData> v : persistentVertexData) {
      graphStore.writeVertexData(v);
    }
    for (PersistentEdgeData<DefaultVertexData> e : persistentEdgeData) {
      graphStore.writeEdgeData(e);
    }

    graphStore.flush();

    // check graph count
    int cnt = 0;
    for (Iterator<DefaultGraphData> graphDataIterator =
         graphStore.getGraphSpace(); graphDataIterator.hasNext(); ) {
      cnt++;
    }
    assertEquals("wrong graph count", persistentGraphData.size(), cnt);

    // check vertex count
    cnt = 0;
    for (Iterator<DefaultVertexData> vertexDataIterator =
         graphStore.getVertexSpace(); vertexDataIterator.hasNext(); ) {
      cnt++;
    }
    assertEquals("wrong vertex count", persistentVertexData.size(), cnt);

    // check edge count
    cnt = 0;
    for (Iterator<DefaultEdgeData> edgeDataIterator =
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
    EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData> graphStore =
      createEmptyEPGMStore();

    PersistentVertexDataFactory<DefaultVertexData, DefaultEdgeData,
      DefaultPersistentVertexData>
      persistentVertexDataFactory = new DefaultPersistentVertexDataFactory();
    VertexDataFactory<DefaultVertexData> vertexDataFactory =
      new DefaultVertexDataFactory();

    // list is not supported by
    final List<String> value = Lists.newArrayList();

    Long vertexID = 0L;
    final String label = "A";
    final Map<String, Object> properties = new HashMap<>();
    properties.put("k1", value);

    final Set<DefaultEdgeData> outEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<DefaultEdgeData> inEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<Long> graphs = Sets.newHashSetWithExpectedSize(0);
    PersistentVertexData<DefaultEdgeData> v = persistentVertexDataFactory
      .createVertexData(
        vertexDataFactory.createVertexData(vertexID, label, properties, graphs),
        outEdges, inEdges);

    graphStore.writeVertexData(v);
  }

  /**
   * Checks if property values are read correctly.
   */
  @Test
  public void propertyTypeTest() {
    EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData> graphStore =
      createEmptyEPGMStore();

    PersistentVertexDataFactory<DefaultVertexData, DefaultEdgeData,
      DefaultPersistentVertexData>
      persistentVertexDataFactory = new DefaultPersistentVertexDataFactory();
    VertexDataFactory<DefaultVertexData> vertexDataFactory =
      new DefaultVertexDataFactory();

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

    final Long vertexID = 0L;
    final String label = "A";

    final Map<String, Object> properties = new HashMap<>();
    properties.put(keyBoolean, valueBoolean);
    properties.put(keyInteger, valueInteger);
    properties.put(keyLong, valueLong);
    properties.put(keyFloat, valueFloat);
    properties.put(keyDouble, valueDouble);
    properties.put(keyString, valueString);

    final Set<DefaultEdgeData> outEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<DefaultEdgeData> inEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<Long> graphs = Sets.newHashSetWithExpectedSize(0);

    // write to store
    graphStore.writeVertexData(persistentVertexDataFactory.createVertexData(
      vertexDataFactory.createVertexData(vertexID, label, properties, graphs),
      outEdges, inEdges));

    graphStore.flush();

    // read from store
    VertexData v = graphStore.readVertexData(vertexID);
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