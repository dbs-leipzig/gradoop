
package org.gradoop.common.storage.api;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;

import java.io.IOException;

/**
 * Responsible for reading and writing edge data from and to HBase.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface EdgeHandler<E extends EPGMEdge, V extends EPGMVertex>
  extends GraphElementHandler {

  /**
   * Adds the source vertex data to the given {@link Put} and returns it.
   *
   * @param put        HBase {@link Put}
   * @param vertexData vertex data
   * @return put with vertex data
   */
  Put writeSource(final Put put, final V vertexData) throws IOException;

  /**
   * Reads the source vertex identifier from the given {@link Result}.
   *
   * @param res HBase {@link Result}
   * @return source vertex identifier
   */
  GradoopId readSourceId(final Result res) throws IOException;

  /**
   * Adds the target vertex data to the given {@link Put} and returns it.
   *
   * @param put        HBase HBase {@link Put}
   * @param vertexData vertex data
   * @return put with vertex data
   */
  Put writeTarget(final Put put, final V vertexData) throws IOException;

  /**
   * Reads the target vertex identifier from the given {@link Result}.
   *
   * @param res HBase {@link Result}
   * @return target vertex identifier
   */
  GradoopId readTargetId(final Result res) throws IOException;

  /**
   * Writes the complete edge data to the given {@link Put} and returns it.
   *
   * @param put      {@link} Put to add edge to
   * @param edgeData edge data to be written
   * @return put with edge data
   */
  Put writeEdge(final Put put, final PersistentEdge<V> edgeData) throws
    IOException;

  /**
   * Reads the edge data from the given {@link Result}.
   *
   * @param res HBase row
   * @return edge data contained in the given result
   */
  E readEdge(final Result res);

  /**
   * Returns the edge data factory used by this handler.
   *
   * @return edge data factory
   */
  EPGMEdgeFactory<E> getEdgeFactory();
}
