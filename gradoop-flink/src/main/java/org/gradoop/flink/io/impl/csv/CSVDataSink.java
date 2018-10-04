/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.csv.functions.EdgeToCSVEdge;
import org.gradoop.flink.io.impl.csv.functions.GraphHeadToCSVGraphHead;
import org.gradoop.flink.io.impl.csv.functions.VertexToCSVVertex;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;
import org.gradoop.flink.io.impl.csv.tuples.CSVEdge;
import org.gradoop.flink.io.impl.csv.tuples.CSVGraphHead;
import org.gradoop.flink.io.impl.csv.tuples.CSVVertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * A graph data sink for CSV files.
 */
public class CSVDataSink extends CSVBase implements DataSink {
  /**
   * Path to meta data file that is used to write the output.
   */
  private final String metaDataPath;

  /**
   * Creates a new CSV data sink. Computes the meta data based on the given graph.
   *
   * @param csvPath directory to write to
   * @param config Gradoop Flink configuration
   */
  public CSVDataSink(String csvPath, GradoopFlinkConfig config) {
    super(csvPath, config);
    this.metaDataPath = null;
  }

  /**
   * Creates a new CSV data sink. Uses the specified meta data to write the CSV output.
   *
   * @param csvPath directory to write CSV files to
   * @param metaDataPath path to meta data CSV file
   * @param config Gradoop Flink configuration
   */
  public CSVDataSink(String csvPath, String metaDataPath, GradoopFlinkConfig config) {
    super(csvPath, config);
    this.metaDataPath = metaDataPath;
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
  public void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException {
    write(logicalGraph.getConfig().getGraphCollectionFactory().fromGraph(logicalGraph), overwrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overwrite) throws IOException {
    FileSystem.WriteMode writeMode = overwrite ?
      FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE;

    DataSet<Tuple3<String, String, String>> metaData;
    if (!reuseMetadata()) {
      metaData = createMetaData(graphCollection);
    } else {
      metaData = MetaData.fromFile(metaDataPath, getConfig());
    }

    DataSet<CSVGraphHead> csvGraphHeads = graphCollection.getGraphHeads()
      .map(new GraphHeadToCSVGraphHead())
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<CSVVertex> csvVertices = graphCollection.getVertices()
      .map(new VertexToCSVVertex())
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<CSVEdge> csvEdges = graphCollection.getEdges()
      .map(new EdgeToCSVEdge())
      .withBroadcastSet(metaData, BC_METADATA);

    // Write metadata only if the path is not the same or reuseMetadata is false.
    if (!getMetaDataPath().equals(metaDataPath) || !reuseMetadata()) {
      metaData.writeAsCsv(getMetaDataPath(), CSVConstants.ROW_DELIMITER,
        CSVConstants.TOKEN_DELIMITER, writeMode).setParallelism(1);
    }

    csvGraphHeads.writeAsCsv(getGraphHeadCSVPath(), CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER, writeMode);

    csvVertices.writeAsCsv(getVertexCSVPath(), CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER, writeMode);

    csvEdges.writeAsCsv(getEdgeCSVPath(), CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER, writeMode);
  }

  /**
   * Returns true, if the meta data shall be reused.
   *
   * @return true, iff reuse is possible
   */
  private boolean reuseMetadata() {
    return this.metaDataPath != null && !this.metaDataPath.isEmpty();
  }
}
