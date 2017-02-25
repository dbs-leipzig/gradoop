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

package org.gradoop.flink.io.impl.csv;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.csv.functions.EdgeToCSV;
import org.gradoop.flink.io.impl.csv.functions.ElementToPropertyMetaData;
import org.gradoop.flink.io.impl.csv.functions.ReducePropertyMetaData;
import org.gradoop.flink.io.impl.csv.functions.VertexToCSV;
import org.gradoop.flink.io.impl.csv.metadata.MetaDataParser;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * A graph data sink for CSV files.
 */
public class CSVDataSink extends CSVBase implements DataSink {

  /**
   * Creates a new CSV data source.
   *
   * @param csvPath directory to write to
   * @param config Gradoop Flink configuration
   */
  public CSVDataSink(String csvPath, GradoopFlinkConfig config) {
    super(csvPath, config);
  }

  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(logicalGraph, false);
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    write(graphCollection, false);
  }

  @Override
  public void write(GraphTransactions graphTransactions) throws IOException {
    write(graphTransactions, false);
  }

  @Override
  public void write(LogicalGraph logicalGraph, boolean overWrite) throws IOException {
    FileSystem.WriteMode writeMode = overWrite ?
      FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE;

    DataSet<Tuple2<String, String>> metaData = createMetaData(logicalGraph);

    DataSet<Tuple3<String, String, String>> csvVertices = logicalGraph.getVertices()
      .map(new VertexToCSV())
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<Tuple5<String, String, String, String, String>> csvEdges = logicalGraph.getEdges()
      .map(new EdgeToCSV())
      .withBroadcastSet(metaData, BC_METADATA);

    // write everything
    metaData.writeAsCsv(getMetaDataPath(), CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER, writeMode).setParallelism(1);

    csvVertices.writeAsCsv(getVertexCSVPath(), CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER, writeMode);

    csvEdges.writeAsCsv(getEdgeCSVPath(), CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER, writeMode);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) throws IOException {
    throw new UnsupportedOperationException(
      "Writing a graph collection is currently not supported by this data sink");
  }

  @Override
  public void write(GraphTransactions graphTransactions, boolean overWrite) throws IOException {
    throw new UnsupportedOperationException(
      "Writing graph transactions is currently not supported by this data sink");
  }

  /**
   * Creates the meta data for the given graph.
   *
   * @param graph logical graph
   * @return meta data information
   */
  private DataSet<Tuple2<String, String>> createMetaData(LogicalGraph graph) {
    return createMetaData(graph.getVertices())
      .union(createMetaData(graph.getEdges()));
  }

  /**
   * Creates the meta data for the specified data set of EPGM elements.
   *
   * @param elements EPGM elements
   * @param <E> EPGM element type
   * @return meta data information
   */
  private <E extends Element> DataSet<Tuple2<String, String>> createMetaData(DataSet<E> elements) {
    return elements
      .map(new ElementToPropertyMetaData<>())
      .groupBy(0)
      .reduceGroup(new ReducePropertyMetaData())
      .map(tuple -> Tuple2.of(tuple.f0, MetaDataParser.getPropertiesMetaData(tuple.f1)))
      .returns(new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
      .withForwardedFields("f0");
  }
}
