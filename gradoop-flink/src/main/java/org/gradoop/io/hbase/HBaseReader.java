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
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.tuples.Subgraph;
import org.gradoop.storage.api.EdgeDataHandler;
import org.gradoop.storage.api.GraphDataHandler;
import org.gradoop.storage.api.VertexDataHandler;

/**
 * Contains classes to read an EPGM database from HBase.
 */
public class HBaseReader {

  /**
   * Reads graph data from HBase.
   *
   * @param <GD> EPGM graph head type
   */
  public static class GraphDataTableInputFormat<GD extends GraphData> extends
    TableInputFormat<Subgraph<Long, GD>> {

    /**
     * Handles reading of persistent graph data.
     */
    private final GraphDataHandler<GD> graphDataHandler;

    /**
     * Table to read from.
     */
    private final String graphDataTableName;

    /**
     * Creates an graph table input format.
     *
     * @param graphDataHandler   graph data handler
     * @param graphDataTableName graph data table name
     */
    public GraphDataTableInputFormat(GraphDataHandler<GD> graphDataHandler,
      String graphDataTableName) {
      this.graphDataHandler = graphDataHandler;
      this.graphDataTableName = graphDataTableName;
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
      return graphDataTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Subgraph<Long, GD> mapResultToTuple(Result result) {
      GD graphData = graphDataHandler.readGraphData(result);
      return new Subgraph<>(graphData.getId(), graphData);
    }
  }

  /**
   * Reads vertex data from HBase.
   *
   * @param <VD> EPGM vertex type
   * @param <ED> EPGM edge type
   */
  public static class VertexDataTableInputFormat<VD extends VertexData, ED
    extends EdgeData> extends
    TableInputFormat<Vertex<Long, VD>> {

    /**
     * Handles reading of persistent vertex data.
     */
    private final VertexDataHandler<VD, ED> vertexDataHandler;

    /**
     * Table to read from.
     */
    private final String vertexDataTableName;

    /**
     * Creates an vertex table input format.
     *
     * @param vertexDataHandler   vertex data handler
     * @param vertexDataTableName vertex data table name
     */
    public VertexDataTableInputFormat(
      VertexDataHandler<VD, ED> vertexDataHandler, String vertexDataTableName) {
      this.vertexDataHandler = vertexDataHandler;
      this.vertexDataTableName = vertexDataTableName;
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
      return vertexDataTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Vertex<Long, VD> mapResultToTuple(Result result) {
      VD vertexData = vertexDataHandler.readVertexData(result);
      return new Vertex<>(vertexData.getId(), vertexData);
    }
  }

  /**
   * Reads edge data from HBase.
   *
   * @param <ED> EPGM edge type
   */
  public static class EdgeDataTableInputFormat<ED extends EdgeData, VD
    extends VertexData> extends
    TableInputFormat<Edge<Long, ED>> {

    /**
     * Handles reading of persistent edge data.
     */
    private final EdgeDataHandler<ED, VD> edgeDataHandler;

    /**
     * Table to read from.
     */
    private final String edgeDataTableName;

    /**
     * Creates an edge table input format.
     *
     * @param edgeDataHandler   edge data handler
     * @param edgeDataTableName edge data table name
     */
    public EdgeDataTableInputFormat(EdgeDataHandler<ED, VD> edgeDataHandler,
      String edgeDataTableName) {
      this.edgeDataHandler = edgeDataHandler;
      this.edgeDataTableName = edgeDataTableName;
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
      return edgeDataTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Edge<Long, ED> mapResultToTuple(Result result) {
      ED edgeData = edgeDataHandler.readEdgeData(result);
      return new Edge<>(edgeData.getSourceVertexId(),
        edgeData.getTargetVertexId(), edgeData);
    }
  }
}
