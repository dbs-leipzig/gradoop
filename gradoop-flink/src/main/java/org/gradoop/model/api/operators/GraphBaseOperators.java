/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.gradoop.io.api.DataSink;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.GraphCollection;

/**
 * Operators that are available at all graph structures.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 *
 * @see LogicalGraph
 * @see GraphCollection
 */
public interface GraphBaseOperators
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  //----------------------------------------------------------------------------
  // Containment methods
  //----------------------------------------------------------------------------

  /**
   * Returns all vertices including vertex data associated with that graph.
   *
   * @return vertices
   */
  DataSet<V> getVertices();

  /**
   * Returns all edge data associated with that logical graph.
   *
   * @return edges
   */
  DataSet<E> getEdges();

  /**
   * Returns the edge data associated with the outgoing edges of the given
   * vertex.
   *
   * @param vertexID vertex identifier
   * @return outgoing edge data of given vertex
   */
  @Deprecated
  DataSet<E> getOutgoingEdges(final GradoopId vertexID);

  /**
   * Returns the edge data associated with the incoming edges of the given
   * vertex.
   *
   * @param vertexID vertex identifier
   * @return incoming edge data of given vertex
   */
  @Deprecated
  DataSet<E> getIncomingEdges(final GradoopId vertexID);

  /**
   * Transforms the EPGM graph to a Gelly Graph.
   *
   * @return Gelly Graph
   */
  Graph<GradoopId, V, E> toGellyGraph();

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which
   * indicates if the collection is empty.
   *
   * A collection is considered empty, if it contains no logical graphs.
   *
   * @return  1-element dataset containing {@code true}, if the collection is
   *          empty or {@code false} if not
   */
  DataSet<Boolean> isEmpty();

  /**
   * Writes the logical graph / graph collection into three separate JSON files.
   * {@code vertexFile} contains all vertices, {@code edgeFile} contains all
   * edges and {@code graphFile} contains the graph data the logical graph /
   * graph collection.
   * <p>
   * Operation uses Flink to write the internal datasets, thus writing to
   * local file system ({@code file://}) as well as HDFS ({@code hdfs://}) is
   * supported.
   *
   * @param vertexFile vertex data output file
   * @param edgeFile   edge data output file
   * @param graphFile  graph data output file
   * @throws Exception
   */
  @Deprecated
  void writeAsJson(final String vertexFile, final String edgeFile,
    final String graphFile) throws Exception;

  /**
   * Writes logical graph/graph collection to given data sink.
   *
   * @param dataSink data sing
   */
  void writeTo(DataSink<G, V, E> dataSink);
}
