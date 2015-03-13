package org.gradoop.io.writer;

import com.google.common.collect.Iterables;
import org.gradoop.GConstants;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Export from Gradoop to Neo4j instance.
 */
public class Neo4jLineWriter implements VertexLineWriter {
  /**
   * Regex for splitting graph values.
   */
  private static final String SPLIT_REGEX = " ";
  /**
   * Neo4j db
   */
  private GraphDatabaseService graphDb;
  /**
   * Access to global node operations
   */
  private GlobalGraphOperations globOps;
  /**
   * index
   */
  private Index<Node> gradoopIdIndex;

  /**
   * Constructor
   * @param dbPath Neo4j db instance
   */
  public Neo4jLineWriter(String dbPath) throws FileNotFoundException {
    if (dbPath.isEmpty()) {
      dbPath = "target/neo4j-db";
    }
    initDb(dbPath);
  }

  @Override
  public String writeVertex(Vertex vertex) {
    Node node = graphDb.createNode();
    node.setProperty(GConstants.GRADOOP_VERTEX_ID_PROPERTY, vertex.getID());
    gradoopIdIndex.add(node, GConstants.GRADOOP_VERTEX_ID_PROPERTY, vertex
      .getID());
    node = writeLabels(node, vertex);
    node = writeProperties(node, vertex);
    node = writeGraphs(node, vertex);

    return node.toString();
  }

  /**
   * Edges are written separate after vertices are written.
   * @param vertex edges are extracted from vertices
   */
  public void writeEdges(Vertex vertex) {

    Node processedNode = gradoopIdIndex.get(
      GConstants.GRADOOP_VERTEX_ID_PROPERTY, vertex.getID()).getSingle();
    //for (Node itNode : globOps.getAllNodes()) {
    //long nodeId = (long) itNode.getProperty(GRADOOP_VERTEX_ID_PROPERTY);
    //if (vertex.getID() == nodeId) {
    writeNodeEdges(processedNode, vertex.getOutgoingEdges());
    //}
    //}
  }

  /**
   * Writes graphs to a specific property in the Neo4j node object.
   * @param node Neo4j node to write to
   * @param vertex vertex
   * @return updated Neo4j node
   */
  private Node writeGraphs(Node node, Vertex vertex) {
    StringBuilder sb = new StringBuilder("");
    for (Long g : vertex.getGraphs()) {
      sb.append(g.toString()).append(SPLIT_REGEX);
    }
    node.setProperty(GConstants.GRAPHS, sb.toString());
    return node;
  }

  /**
   * Writes relationships (edges) to the Neo4j node object.
   * @param node Neo4j node to write to
   * @param outEdges outEdges
   * @return updated Neo4j node
   */
  private Node writeNodeEdges(Node node, Iterable<Edge> outEdges) {
    for (Edge edge : outEdges) {
      for (Node targetNode : globOps.getAllNodes()) {
        if (targetNode.getProperty(GConstants.GRADOOP_VERTEX_ID_PROPERTY)
          .equals(edge.getOtherID())) {
          RelationshipType relType =
            DynamicRelationshipType.withName(edge.getLabel());
          Relationship relationship = node.createRelationshipTo(targetNode,
            relType);
          if (edge.getPropertyCount() > 0) {
            Iterable<String> edgeProps = edge.getPropertyKeys();
            for (String key : edgeProps) {
              relationship.setProperty(key, edge.getProperty(key));
            }
          }
        }
      }
    }
    return node;
  }

  /**
   * Writes vertex properties to the Neo4j node object.
   * @param node Neo4j node to write to
   * @param vertex vertex
   * @return updated Neo4j node
   */
  private Node writeProperties(Node node, Vertex vertex) {
    for (String key : vertex.getPropertyKeys()) {
      node.setProperty(key, vertex.getProperty(key));
    }

    return node;
  }

  /**
   * Writes vertex labels to the given Neo4j node object.
   * @param node Neo4j node to write to
   * @param vertex vertex
   * @return updated Neo4j node
   */
  private Node writeLabels(Node node, Vertex vertex) {
    for (String l : vertex.getLabels()) {
      node.addLabel(DynamicLabel.label(l));
    }

    return node;
  }

  /**
   * Get all nodes from graph db.
   * @return Nodes
   */
  protected Iterable<Node> getNodes() {
    return globOps.getAllNodes();
  }

  /**
   * Get node count for current graph db.
   * @return node count
   */
  protected int getNodeCount() {
    return (getNodes() != null) ? Iterables.size(getNodes()) : 0;
  }

  /**
   * Init graph database and register global graph operations.
   * @param dbPath path to Neo4j database instance
   */
  protected void initDb(String dbPath) throws FileNotFoundException {
    deleteFileOrDirectory(new File(dbPath));
    graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
    globOps = GlobalGraphOperations.at(graphDb);
    try (Transaction tx = graphDb.beginTx()) {
      gradoopIdIndex =
        graphDb.index().forNodes(GConstants.GRADOOP_VERTEX_ID_PROPERTY);
      tx.success();
    }
    registerShutdownHook(graphDb);
  }

  /**
   * Get graph database service.
   * @return gdbs
   */
  public GraphDatabaseService getGraphDbService() {
    return graphDb;
  }
//
//  protected void removeData() {
//    try (Transaction tx = graphDb.beginTx()) {
////      firstNode.getSingleRelationship(RelTypes.KNOWS, Direction.OUTGOING)
////        .delete();
////      firstNode.delete();
////      secondNode.delete();
//      tx.success();
//    }
//  }

  /**
   * Shut down graph database service.
   */
  public void shutdown() {
    graphDb.shutdown();
  }

  /**
   * Shut down helper.
   * @param graphDb foo
   */
  private static void registerShutdownHook(final GraphDatabaseService graphDb) {
    // Registers a shutdown hook for the Neo4j instance so that it
    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
    // running application).
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        graphDb.shutdown();
      }
    });
  }

  /**
   * Delete existing database on startup.
   * @param file param
   */
  private static void deleteFileOrDirectory(File file) throws
    NullPointerException, FileNotFoundException {
    if (file.exists()) {
      if (file.isDirectory()) {
        for (File child : file.listFiles()) {
          if (child == null) {
            break;
          } else {
            deleteFileOrDirectory(child);
          }
        }
      }
      if (!file.delete()) {
        // recover from error or throw an exception
        throw new FileNotFoundException("Failed to delete file " + file);
      }
    }
  }

  /**
   * Create all vertices and edges in the Neo4j database for given graph store.
   * @param graphStore graph store containing vertices and edges
   */
  public void produceOutput(GraphStore graphStore) {
    try (Transaction tx = graphDb.beginTx()) {
      for (Vertex v : graphStore.readVertices()) {
        writeVertex(v);
      }
      tx.success();
    }

    try (Transaction tx = graphDb.beginTx()) {
      for (Vertex v : graphStore.readVertices()) {
        writeEdges(v);
      }
      tx.success();
    }
  }
}
