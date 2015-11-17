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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.hbase;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.tuples.Subgraph;
import org.gradoop.storage.api.EdgeHandler;
import org.gradoop.storage.api.GraphHeadHandler;
import org.gradoop.storage.api.VertexHandler;

/**
 * Contains classes to read an EPGM database from HBase.
 */
public class HBaseReader {

  /**
   * Reads graph data from HBase.
   *
   * @param <GD> EPGM graph head type
   */
  public static class GraphHeadTableInputFormat<GD extends EPGMGraphHead>
    extends TableInputFormat<Subgraph<GradoopId, GD>> {

    /**
     * Handles reading of persistent graph data.
     */
    private final GraphHeadHandler<GD> graphHeadHandler;

    /**
     * Table to read from.
     */
    private final String graphHeadTableName;

    /**
     * Creates an graph table input format.
     *
     * @param graphHeadHandler   graph data handler
     * @param graphHeadTableName graph data table name
     */
    public GraphHeadTableInputFormat(GraphHeadHandler<GD> graphHeadHandler,
      String graphHeadTableName) {
      this.graphHeadHandler = graphHeadHandler;
      this.graphHeadTableName = graphHeadTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Scan getScanner() {
      Scan scan = new Scan();
      scan.setCaching(GConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
      return scan;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getTableName() {
      return graphHeadTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Subgraph<GradoopId, GD> mapResultToTuple(Result result) {
      GD graphData = graphHeadHandler.readGraphHead(result);
      return new Subgraph<>(graphData.getId(), graphData);
    }
  }

  /**
   * Reads vertex data from HBase.
   *
   * @param <VD> EPGM vertex type
   * @param <ED> EPGM edge type
   */
  public static class VertexTableInputFormat<VD extends EPGMVertex, ED
    extends EPGMEdge> extends
    TableInputFormat<Vertex<GradoopId, VD>> {

    /**
     * Handles reading of persistent vertex data.
     */
    private final VertexHandler<VD, ED> vertexHandler;

    /**
     * Table to read from.
     */
    private final String vertexTableName;

    /**
     * Creates an vertex table input format.
     *
     * @param vertexHandler   vertex data handler
     * @param vertexTableName vertex data table name
     */
    public VertexTableInputFormat(VertexHandler<VD, ED> vertexHandler,
      String vertexTableName) {
      this.vertexHandler = vertexHandler;
      this.vertexTableName = vertexTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Scan getScanner() {
      Scan scan = new Scan();
      scan.setCaching(GConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
      return scan;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getTableName() {
      return vertexTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Vertex<GradoopId, VD> mapResultToTuple(Result result) {
      VD vertexData = vertexHandler.readVertex(result);
      return new Vertex<>(vertexData.getId(), vertexData);
    }
  }

  /**
   * Reads edge data from HBase.
   *
   * @param <ED> EPGM edge type
   */
  public static class EdgeTableInputFormat<ED extends EPGMEdge, VD
    extends EPGMVertex> extends
    TableInputFormat<Edge<GradoopId, ED>> {

    /**
     * Handles reading of persistent edge data.
     */
    private final EdgeHandler<ED, VD> edgeHandler;

    /**
     * Table to read from.
     */
    private final String edgeTableName;

    /**
     * Creates an edge table input format.
     *
     * @param edgeHandler   edge data handler
     * @param edgeTableName edge data table name
     */
    public EdgeTableInputFormat(EdgeHandler<ED, VD> edgeHandler,
      String edgeTableName) {
      this.edgeHandler = edgeHandler;
      this.edgeTableName = edgeTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Scan getScanner() {
      Scan scan = new Scan();
      scan.setCaching(GConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
      return scan;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getTableName() {
      return edgeTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Edge<GradoopId, ED> mapResultToTuple(Result result) {
      ED edgeData = edgeHandler.readEdge(result);
      return new Edge<>(edgeData.getSourceVertexId(),
        edgeData.getTargetVertexId(), edgeData);
    }
  }
}
