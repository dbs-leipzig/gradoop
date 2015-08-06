///*
// * This file is part of Gradoop.
// *
// * Gradoop is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * Gradoop is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
// */
//
//package org.gradoop.io.writer;
//
//import org.apache.log4j.Logger;
//import org.gradoop.GConstants;
//import org.gradoop.model.EdgeData;
//import org.gradoop.model.VertexData;
//import org.neo4j.graphdb.DynamicLabel;
//import org.neo4j.graphdb.DynamicRelationshipType;
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.Node;
//import org.neo4j.graphdb.Relationship;
//import org.neo4j.graphdb.RelationshipType;
//import org.neo4j.graphdb.Transaction;
//import org.neo4j.graphdb.factory.GraphDatabaseFactory;
//import org.neo4j.graphdb.index.Index;
//import org.neo4j.helpers.collection.IteratorUtil;
//import org.neo4j.tooling.GlobalGraphOperations;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//
///**
// * Export from Gradoop to Neo4j instance.
// */
//public class Neo4jLineWriter implements VertexLineWriter {
//  /**
//   * Class logger.
//   */
//  private static final Logger LOG = Logger.getLogger(Neo4jLineWriter.class);
//  /**
//   * Regex for splitting graph values.
//   */
//  private static final String SPLIT_REGEX = " ";
//  /**
//   * Neo4j db
//   */
//  private GraphDatabaseService graphDb;
//  /**
//   * Access to global node operations
//   */
//  private GlobalGraphOperations globOps;
//  /**
//   * index
//   */
//  private Index<Node> gradoopIdIndex;
//
//  /**
//   * Constructor
//   * @param dbPath Neo4j db instance
//   */
//  public Neo4jLineWriter(String dbPath) throws FileNotFoundException {
//    if (dbPath.isEmpty()) {
//      dbPath = "output/neo4j-db";
//    }
//    initDb(dbPath);
//  }
//
//  @Override
//  public String writeVertex(VertexData vertexData) {
//    Node node = graphDb.createNode();
//    node.setProperty(GConstants.GRADOOP_VERTEX_ID_PROPERTY, vertexData.getId());
//    gradoopIdIndex.add(node, GConstants.GRADOOP_VERTEX_ID_PROPERTY, vertexData
//      .getId());
//    node = writeLabels(node, vertexData);
//    node = writeProperties(node, vertexData);
//    node = writeGraphs(node, vertexData);
//
//    return node.toString();
//  }
//
//  /**
//   * Edges are written separate after vertices are written.
//   * @param vertexData edges are extracted from vertices
//   */
//  public void writeEdges(VertexData vertexData) {
//    Node node = gradoopIdIndex.get(GConstants.GRADOOP_VERTEX_ID_PROPERTY,
//      vertexData.getId()).getSingle();
//    if (vertexData.getOutgoingDegree() > 0) {
//      for (EdgeData edgeData : vertexData.getOutgoingEdges()) {
//        LOG.info("edge.getOtherID() " + edgeData.getOtherID());
//        LOG.info("node.getLabel() " + gradoopIdIndex.get(GConstants
//            .GRADOOP_VERTEX_ID_PROPERTY, edgeData.getOtherID()).getSingle());
//      }
//      writeNodeRelationships(node, vertexData.getOutgoingEdges());
//    }
//  }
//
//  /**
//   * Writes graphs to a specific property in the Neo4j node object.
//   * @param node Neo4j node to write to
//   * @param vertexData vertex
//   * @return updated Neo4j node
//   */
//  private Node writeGraphs(Node node, VertexData vertexData) {
//    StringBuilder sb = new StringBuilder("");
//    for (Long g : vertexData.getGraphs()) {
//      sb.append(g.toString()).append(SPLIT_REGEX);
//    }
//    node.setProperty(GConstants.GRAPHS, sb.toString());
//    return node;
//  }
//
//  /**
//   * Writes relationships (edges) to the Neo4j node object.
//   * @param node Neo4j node to write to
//   * @param outEdges outEdges
//   * @return updated Neo4j node
//   */
//  private Node writeNodeRelationships(Node node, Iterable<EdgeData> outEdges) {
//    for (EdgeData edgeData : outEdges) {
//      Node target = gradoopIdIndex.get(GConstants.GRADOOP_VERTEX_ID_PROPERTY,
//        edgeData.getOtherID()).getSingle();
//      LOG.info("node: " + node.getLabels().iterator().next());
//      LOG.info("target: " + target.toString());
//      LOG.info("target: " + target.getLabels().iterator().next());
//      RelationshipType relType =
//        DynamicRelationshipType.withName(edgeData.getLabel());
//      Relationship relationship = node.createRelationshipTo(target, relType);
//      if (edgeData.getPropertyCount() > 0) {
//        Iterable<String> edgeProps = edgeData.getPropertyKeys();
//        for (String key : edgeProps) {
//          relationship.setProperty(key, edgeData.getProperty(key));
//        }
//      }
//    }
//    return node;
//  }
//
//  /**
//   * Writes vertex properties to the Neo4j node object.
//   * @param node Neo4j node to write to
//   * @param vertexData vertex
//   * @return updated Neo4j node
//   */
//  private Node writeProperties(Node node, VertexData vertexData) {
//    for (String key : vertexData.getPropertyKeys()) {
//      node.setProperty(key, vertexData.getProperty(key));
//    }
//
//    return node;
//  }
//
//  /**
//   * Writes vertex labels to the given Neo4j node object.
//   * @param node Neo4j node to write to
//   * @param vertexData vertex
//   * @return updated Neo4j node
//   */
//  private Node writeLabels(Node node, VertexData vertexData) {
//    node.addLabel(DynamicLabel.label(vertexData.getLabel()));
//
//    return node;
//  }
//
//  /**
//   * Get all nodes from graph db.
//   * @return Nodes
//   */
//  protected Iterable<Node> getNodes() {
//    return globOps.getAllNodes();
//  }
//
//  /**
//   * Get node count for current graph db.
//   * @return node count
//   */
//  public int getNodeCount() {
//    int result;
//    try (Transaction tx = graphDb.beginTx()) {
//      if (getNodes() != null) {
//        result = IteratorUtil.count(getNodes());
//      } else {
//        result = 0;
//      }
//      tx.success();
//    }
//    return result;
//  }
//
//  /**
//   * Init graph database and register global graph operations.
//   * @param dbPath path to Neo4j database instance
//   */
//  protected void initDb(String dbPath) throws FileNotFoundException {
//    deleteFileOrDirectory(new File(dbPath));
//    graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
//    globOps = GlobalGraphOperations.at(graphDb);
//    try (Transaction tx = graphDb.beginTx()) {
//      gradoopIdIndex =
//        graphDb.index().forNodes(GConstants.GRADOOP_VERTEX_ID_PROPERTY);
//      tx.success();
//    }
//    registerShutdownHook(graphDb);
//  }
//
//  /**
//   * Get graph database service.
//   * @return gdbs
//   */
//  public GraphDatabaseService getGraphDbService() {
//    return graphDb;
//  }
////
////  protected void removeData() {
////    try (Transaction tx = graphDb.beginTx()) {
//////      firstNode.getSingleRelationship(RelTypes.KNOWS, Direction.OUTGOING)
//////        .delete();
//////      firstNode.delete();
//////      secondNode.delete();
////      tx.success();
////    }
////  }
//
//  /**
//   * Shut down graph database service.
//   */
//  public void shutdown() {
//    graphDb.shutdown();
//  }
//
//  /**
//   * Shut down helper.
//   * @param graphDb foo
//   */
//  private static void registerShutdownHook(final GraphDatabaseService graphDb) {
//    // Registers a shutdown hook for the Neo4j instance so that it
//    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
//    // running application).
//    Runtime.getRuntime().addShutdownHook(new Thread() {
//      @Override
//      public void run() {
//        graphDb.shutdown();
//      }
//    });
//  }
//
//  /**
//   * Delete existing database on startup.
//   * @param file param
//   */
//  private static void deleteFileOrDirectory(File file) throws
//    NullPointerException, FileNotFoundException {
//    if (file.exists()) {
//      if (file.isDirectory()) {
//        for (File child : file.listFiles()) {
//          if (child == null) {
//            break;
//          } else {
//            deleteFileOrDirectory(child);
//          }
//        }
//      }
//      if (!file.delete()) {
//        // recover from error or throw an exception
//        throw new FileNotFoundException("Failed to delete file " + file);
//      }
//    }
//  }
//}
