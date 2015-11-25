package org.gradoop.storage.impl.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.GradoopHBaseTestBase;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMIdentifiable;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.id.generators.TestSequenceIdGenerator;
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
import org.gradoop.util.AsciiGraphLoader;
import org.gradoop.util.GradoopConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

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
    validateEPGMElements(
      graphHeads,
      Lists.newArrayList(graphStore.getGraphSpace())
    );
    // vertices
    validateEPGMElements(
      vertices,
      Lists.newArrayList(graphStore.getVertexSpace())
    );
    validateEPGMGraphElements(
      vertices,
      Lists.newArrayList(graphStore.getVertexSpace())
    );
    // edges
    validateEPGMElements(
      edges,
      Lists.newArrayList(graphStore.getEdgeSpace())
    );
    validateEPGMGraphElements(
      edges,
      Lists.newArrayList(graphStore.getEdgeSpace())
    );

    graphStore.close();
  }

  private void validateEPGMElements(
    Collection<? extends EPGMElement> referenceElements,
    Collection<? extends EPGMElement> testElements) {
    List<? extends EPGMElement> referenceList =
      Lists.newArrayList(referenceElements);
    List<? extends EPGMElement> testList =
      Lists.newArrayList(testElements);

    Collections.sort(referenceList, comparator);
    Collections.sort(testList, comparator);

    Iterator<? extends EPGMElement> referenceIterator =
      referenceList.iterator();
    Iterator<? extends EPGMElement> testIterator =
      testList.iterator();

    while(referenceIterator.hasNext()) {
      validateIdLabelAndProperties(
        referenceIterator.next(),
        testIterator.next());
    }
    assertFalse(referenceIterator.hasNext());
    assertFalse(testIterator.hasNext());
  }

  private void validateEPGMGraphElements(
    Collection<? extends EPGMGraphElement> referenceElements,
    Collection<? extends EPGMGraphElement> testElements) {
    List<? extends EPGMGraphElement> referenceList =
      Lists.newArrayList(referenceElements);
    List<? extends EPGMGraphElement> testList =
      Lists.newArrayList(testElements);

    Collections.sort(referenceList, comparator);
    Collections.sort(testList, comparator);

    Iterator<? extends EPGMGraphElement> referenceIterator =
      referenceList.iterator();
    Iterator<? extends EPGMGraphElement> testIterator =
      testList.iterator();

    while(referenceIterator.hasNext()) {
      validateGraphContainment(
        referenceIterator.next(),
        testIterator.next());
    }
    assertFalse(referenceIterator.hasNext());
    assertFalse(testIterator.hasNext());
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

    GradoopId vertexID = new TestSequenceIdGenerator().createId();
    final String label = "A";
    final Map<String, Object> properties = new HashMap<>();
    properties.put("k1", value);

    final Set<EdgePojo> outEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<EdgePojo> inEdges = Sets.newHashSetWithExpectedSize(0);
    final GradoopIdSet graphs = new GradoopIdSet();
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

    final GradoopId vertexID = new TestSequenceIdGenerator().createId();
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
    final GradoopIdSet graphs = new GradoopIdSet();

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

  public void validateGraphHead(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore,
    GraphHeadPojo originalGraphHead) {

    EPGMGraphHead loadedGraphHead = graphStore
      .readGraph(originalGraphHead.getId());

    validateIdLabelAndProperties(originalGraphHead, loadedGraphHead);
  }

  public void validateVertex(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore,
    VertexPojo originalVertex) {

    EPGMVertex loadedVertex = graphStore.readVertex(originalVertex.getId());

    validateIdLabelAndProperties(originalVertex, loadedVertex);
    validateGraphContainment(originalVertex, loadedVertex);
  }

  public void validateEdge(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore,
    EdgePojo originalEdge) {

    EPGMEdge loadedEdge = graphStore.readEdge(originalEdge.getId());

    validateIdLabelAndProperties(originalEdge, loadedEdge);
    validateGraphContainment(originalEdge, loadedEdge);

    assertTrue(
      "source vertex mismatch",
      originalEdge.getSourceVertexId().equals(
        loadedEdge.getSourceVertexId())
    );

    assertTrue(
      "target vertex mismatch",
      originalEdge.getTargetVertexId().equals(
        loadedEdge.getTargetVertexId()
      )
    );
  }

  private void validateIdLabelAndProperties(EPGMElement originalElement,
    EPGMElement loadedElement) {

    assertNotNull("loading results to NULL", loadedElement);

    assertEquals(
      "id mismatch",
      originalElement.getId(),
      loadedElement.getId()
    );

    assertEquals(
      "label mismatch",
      originalElement.getLabel(),
      loadedElement.getLabel()
    );

    if (originalElement.getPropertyCount() == 0) {
      assertEquals("property count mismatch",
        originalElement.getPropertyCount(),
        loadedElement.getPropertyCount());
    } else {

      List<String> originalKeys = Lists.newArrayList(
        originalElement.getPropertyKeys());
      Collections.sort(originalKeys);

      List<String> loadedKeys = Lists.newArrayList(
        loadedElement.getPropertyKeys());
      Collections.sort(loadedKeys);

      Iterator<String> originalKeysIt = originalKeys.iterator();

      Iterator<String> loadedKeysIt = loadedKeys.iterator();

      while (originalKeysIt.hasNext() && loadedKeysIt.hasNext()) {
        String originalKey = originalKeysIt.next();
        String loadedKey = loadedKeysIt.next();
        assertEquals("property key mismatch", originalKey, loadedKey);
        assertEquals("property value mismatch",
          originalElement.getProperty(originalKey),
          loadedElement.getProperty(loadedKey));
      }
      assertFalse(
        "property count mismatch",
        originalKeysIt.hasNext() || loadedKeysIt.hasNext()
      );
    }
  }

  private void validateGraphContainment(
    EPGMGraphElement originalGraphElement,
    EPGMGraphElement loadedGraphElement) {
    assertTrue(
      "graph containment mismatch",
      originalGraphElement.getGraphIds()
        .equals(loadedGraphElement.getGraphIds())
    );
  }

  private Comparator<EPGMIdentifiable> comparator =
    new Comparator<EPGMIdentifiable>() {
    @Override
    public int compare(EPGMIdentifiable o1, EPGMIdentifiable o2) {
      return o1.getId().compareTo(o2.getId());
    }
  };
}