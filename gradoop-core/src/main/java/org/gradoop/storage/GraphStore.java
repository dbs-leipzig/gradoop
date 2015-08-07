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
//package org.gradoop.storage;
//
//import org.gradoop.model.EdgeData;
//import org.gradoop.model.GraphData;
//import org.gradoop.model.VertexData;
//import org.gradoop.storage.hbase.GraphHandler;
//import org.gradoop.storage.hbase.VertexHandler;
//
//import java.io.IOException;
//import java.util.Iterator;
//
///**
// * A graph store is responsible for writing and reading graphs including
// * vertices and edges.
// */
//public interface GraphStore {
//  /**
//   * Returns the vertex handler used by this store.
//   *
//   * @return vertex handler
//   */
//  VertexHandler getVertexHandler();
//  /**
//   * Returns the graph handler used by this store.
//   *
//   * @return graph handler
//   */
//  GraphHandler getGraphHandler();
//  /**
//   * Writes the given graph into the graph store.
//   *
//   * @param graphData graph to write
//   */
//  void writeGraph(final GraphData graphData);
//
//  /**
//   * Writes the given vertex into the graph store.
//   *
//   * @param vertexData vertex to write
//   */
//  void writeVertex(final VertexData vertexData);
//
//  /**
//   * Reads a graph entity from the graph store using the given graph id. If
//   * {@code graphID} does not exist, {@code null} is returned.
//   *
//   * @param graphID graph id
//   * @return graph entity or {@code null} if there is no entity with the given
//   * {@code graphId}
//   */
//  GraphData readGraph(final Long graphID);
//
//  /**
//   * Reads a vertex entity from the graph store using the given vertex id. If
//   * {@code vertexID} does not exist, {@code null} is returned.
//   *
//   * @param vertexID vertex id
//   * @return vertex entity or {@code null} if there is no entity with the
// given
//   * {@code vertexID}
//   */
//  VertexData readVertex(final Long vertexID);
//
//  /**
//   * Reads all vertices from the graph store. If graph store is empty, {@code
//   * null} is returned.
//   * @param tableName HBase table name
//   * @return all vertices or {@code null} if graph store is empty
//   */
//  Iterator<VertexData> getVertices(String tableName) throws
// InterruptedException,
//    IOException, ClassNotFoundException;
//
//  /**
//   * Reads all vertices from the graph store. If graph store is empty, {@code
//   * null} is returned.
//   * @param tableName HBase table name
//   * @param cacheSize cache size for HBase scan
//   * @return all vertices or {@code null} if graph store is empty
//   */
//  Iterator<VertexData> getVertices(String tableName, int cacheSize) throws
//    InterruptedException, IOException, ClassNotFoundException;
//
//  /**
//   * Reads all edges from the graph store. If no edges exist, {@code null} is
//   * returned
//   * @return all edges or {@code null} if vertices have no edges
//   */
//  Iterable<EdgeData> getEdges();
//
//  /**
//   * Get row count for a table in the graph store
//   * @param tableName HBase table name
//   * @return row count
//   */
//  long getRowCount(String tableName) throws IOException,
// ClassNotFoundException,
//    InterruptedException;
//
//  /**
//   * Setting this value to true, forces the store implementation to flush the
//   * write buffers after every write.
//   *
//   * @param autoFlush true to enable auto flush, false to disable
//   */
//  void setAutoFlush(boolean autoFlush);
//
//  /**
//   * Flushes all buffered writes to the store.
//   */
//  void flush();
//
//  /**
//   * Closes the graph store and flushes all writes.
//   */
//  void close();
//
//  /**
//   * Reads all graphs from the graph store. If graph store is empty, {@code
//   * null} is returned.
//   * @param graphsTableName HBase graphs table name
//   * @return all graphs or {@code null} if graph store is empty
//   */
//  Iterator<GraphData> getGraphs(String graphsTableName);
//}
