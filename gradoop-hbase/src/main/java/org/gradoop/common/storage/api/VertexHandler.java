
package org.gradoop.common.storage.api;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Responsible for reading and writing vertex data from and to HBase.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface VertexHandler<V extends EPGMVertex, E extends EPGMEdge>
  extends GraphElementHandler {

  /**
   * Adds the given outgoing edge data to the given {@link Put} and
   * returns it.
   *
   * @param put   {@link Put} to add edge identifiers to
   * @param edges edges to add
   * @return put with edge identifiers
   */
  Put writeOutgoingEdges(final Put put, final Set<E> edges) throws IOException;

  /**
   * Adds the given incoming edge data to the given {@link Put} and
   * returns it.
   *
   * @param put   {@link Put} to add edge identifiers to
   * @param edges edge identifiers to add
   * @return put with edge identifiers
   */
  Put writeIncomingEdges(final Put put, final Set<E> edges) throws IOException;

  /**
   * Reads the outgoing edge identifiers from the given {@link Result}.
   *
   * @param res HBase row
   * @return outgoing edge identifiers
   */
  Set<Long> readOutgoingEdgeIds(final Result res);

  /**
   * Reads the incoming edge identifiers from the given {@link Result}.
   *
   * @param res HBase row
   * @return incoming edge identifiers
   */
  Set<Long> readIncomingEdgeIds(final Result res);

  /**
   * Writes the complete vertex data to the given {@link Put} and returns it.
   *
   * @param put        {@link Put} to add vertex to
   * @param vertexData vertex data to be written
   * @return put with vertex data
   */
  Put writeVertex(final Put put, final PersistentVertex<E> vertexData) throws
    IOException;

  /**
   * Reads the vertex data from the given {@link Result}.
   *
   * @param res HBase row
   * @return vertex data contained in the given result.
   */
  V readVertex(final Result res);

  /**
   * Returns the vertex data factory used by this handler.
   *
   * @return vertex data factory
   */
  EPGMVertexFactory<V> getVertexFactory();
}
