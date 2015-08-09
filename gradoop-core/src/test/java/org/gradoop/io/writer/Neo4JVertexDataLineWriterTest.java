//package org.gradoop.io.writer;
//
//import com.google.common.collect.Lists;
//import org.gradoop.GConstants;
//import org.gradoop.GradoopTest;
//import org.gradoop.model.VertexData;
//import org.junit.Test;
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.Label;
//import org.neo4j.graphdb.Node;
//import org.neo4j.graphdb.Relationship;
//import org.neo4j.graphdb.Transaction;
//
//import java.io.FileNotFoundException;
//import java.util.List;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;
//
///**
// * Test Neo4jWriter, no Hadoop/HDFS
// */
//public class Neo4JVertexDataLineWriterTest extends GradoopTest {
//  //private static Logger LOG = Logger.getLogger(Neo4jWriterTest.class);
//  private static final String DB_PATH = "target/neo4j-db";
//  private static final String SPLIT_REGEX = " ";
//
//  @Test
//  public void extendedGraphNeo4jTest() throws FileNotFoundException {
//    Neo4jLineWriter writer = new Neo4jLineWriter(DB_PATH);
//    GraphDatabaseService db = writer.getGraphDbService();
//
//    try (Transaction tx = db.beginTx()) {
//      for (VertexData v : createExtendedGraphVertices()) {
//        writer.writeVertexData(v);
//      }
//      validateNodes(writer);
//      tx.success();
//    }
//
//    try (Transaction tx = db.beginTx()) {
//      for (VertexData v : createExtendedGraphVertices()) {
//        writer.writeEdges(v);
//      }
//      validateEdges(writer);
//      tx.success();
//    }
//    //writer.removeData();
//    writer.shutdown();
//  }
//
//  protected void validateNodes(Neo4jLineWriter writer) {
//    assertEquals(3, writer.getNodeCount());
//    for (Node node : writer.getNodes()) {
//      Long gradoopId = (long) node.getProperty(GConstants
//        .GRADOOP_VERTEX_ID_PROPERTY);
//      List<String> labels = Lists.newArrayList();
//      for (Label label : node.getLabels()) {
//        labels.add(label.name());
//      }
//      List<Long> graphs = Lists.newArrayList();
//      String graphElements = node.getProperty(GConstants.GRAPHS).toString();
//      for (String graph : graphElements.split(SPLIT_REGEX)) {
//        graphs.add(Long.valueOf(graph));
//      }
//
//      if (gradoopId.equals(0L)) {
//        assertEquals(1, labels.size());
//        assertTrue(labels.contains("A"));
//        // properties (3 k1 5 v1 k2 5 v2 k3 5 v3)
//        testProperties(node, 3);
//        assertEquals(1, graphs.size());
//        assertTrue(graphs.contains(0L));
//      } else if (gradoopId.equals(1L)) {
//        assertEquals(1, labels.size());
//        assertTrue(labels.contains("B"));
//        // properties (2 k1 5 v1 k2 5 v2)
//        testProperties(node, 2);
//        assertEquals(2, graphs.size());
//        assertTrue(graphs.contains(0L));
//        assertTrue(graphs.contains(1L));
//      } else if (gradoopId.equals(2L)) {
//        assertEquals(1, labels.size());
//        assertTrue(labels.contains("C"));
//        // properties (2 k1 5 v1 k2 5 v2)
//        testProperties(node, 2);
//        assertEquals(1, graphs.size());
//        assertTrue(graphs.contains(1L));
//      } else {
//        assertTrue(false);
//      }
//    }
//  }
//
//  private void validateEdges(Neo4jLineWriter writer) {
//    for (Node node : writer.getNodes()) {
//      Long gradoopId =
//        (long) node.getProperty(GConstants.GRADOOP_VERTEX_ID_PROPERTY);
//      if (gradoopId.equals(0L)) {
//        testEdges(node, 2);
//      } else if (gradoopId.equals(1L)) {
//        testEdges(node, 3);
//      } else if (gradoopId.equals(2L)) {
//        // node degree -1 because of self-referencing node
//        testEdges(node, 2);
//      } else {
//        assertTrue(false);
//      }
//    }
//  }
//
//  private void testEdges(Node node, int expEdgeCount) {
//    long nodeId =
//      (long) node.getProperty(GConstants.GRADOOP_VERTEX_ID_PROPERTY);
//    Iterable<Relationship> nodeRels = node.getRelationships();
//
//    assertEquals(expEdgeCount, node.getDegree());
//    if (expEdgeCount == 0) {
//      assertNull(nodeRels);
//    } else {
//      for (Relationship rel : nodeRels) {
//        boolean outgoing = false;
//        long otherId = (long) rel.getOtherNode(node)
//          .getProperty(GConstants.GRADOOP_VERTEX_ID_PROPERTY);
//        long startId = (long) rel.getStartNode()
//          .getProperty(GConstants.GRADOOP_VERTEX_ID_PROPERTY);
//        if (startId == nodeId) {
//          outgoing = true;
//        }
//        String label = rel.getType().name();
//        testEdge(nodeId, otherId, label, outgoing);
//      }
//    }
//  }
//
//  private void testEdge(Long nodeId, Long otherId, String label,
//    boolean outgoing) {
//    if (nodeId.equals(0L)) {
//      if (outgoing) {
//        assertEquals(1L, (long) otherId);
//        assertEquals("a", label);
//      } else {
//        assertEquals(1L, (long) otherId);
//        assertEquals("b", label);
//      }
//      // out edges (a.1.0 1 k1 5 v1)
//      // in edges (b.1.0 1 k1 5 v1)
//    } else if (nodeId.equals(1L)) {
//      if (outgoing) {
//        if (otherId == 0L) {
//          assertEquals(0L, (long) otherId);
//          assertEquals("b", label);
//        } else {
//          assertEquals(2L, (long) otherId);
//          assertEquals("c", label);
//        }
//      } else {
//        assertEquals(0L, (long) otherId);
//        assertEquals("a", label);
//      }
//      // out edges (b.0.0 2 k1 5 v1 k2 5 v2,c.2.1 0)
//      // in edges (a.0.0 1 k1 5 v1)
//    } else if (nodeId.equals(2L)) {
//      if (outgoing) {
//        assertEquals(2L, (long) otherId);
//        assertEquals("d", label);
//      } else {
//        assertEquals(1L, (long) otherId);
//        assertEquals("c", label);
//      }
//      // out edges (d.2.0 0)
//      // in edges (d.2.0 0,c.2.1 0)
//    } else {
//      assertTrue(false);
//    }
//  }
//
//  private void testProperties(Node node, int expCount) {
//    if (expCount == 0) {
//      assertNull(node.getPropertyKeys());
//    } else {
//      for (String propertyKey : node.getPropertyKeys()) {
//        switch (propertyKey) {
//        case KEY_1:
//          assertEquals(VALUE_1, node.getProperty(KEY_1));
//          break;
//        case KEY_2:
//          assertEquals(VALUE_2, node.getProperty(KEY_2));
//          break;
//        case KEY_3:
//          assertEquals(VALUE_3, node.getProperty(KEY_3));
//          break;
//        }
//      }
//    }
//  }
//}
