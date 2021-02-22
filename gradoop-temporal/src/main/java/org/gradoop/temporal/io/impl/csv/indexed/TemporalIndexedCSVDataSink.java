/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.io.impl.csv.indexed;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.gradoop.flink.io.impl.csv.CSVBase;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.indexed.functions.IndexedCSVFileFormat;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaDataSink;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaDataSource;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.io.impl.csv.functions.TemporalEdgeToTemporalCSVEdge;
import org.gradoop.temporal.io.impl.csv.functions.TemporalGraphHeadToTemporalCSVGraphHead;
import org.gradoop.temporal.io.impl.csv.functions.TemporalVertexToTemporalCSVVertex;
import org.gradoop.temporal.io.impl.csv.tuples.TemporalCSVEdge;
import org.gradoop.temporal.io.impl.csv.tuples.TemporalCSVGraphHead;
import org.gradoop.temporal.io.impl.csv.tuples.TemporalCSVVertex;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;

import java.io.IOException;

/**
 * A temporal graph data sink for CSV files indexed by label.
 */
public class TemporalIndexedCSVDataSink extends CSVBase implements TemporalDataSink {
  /**
   * Path to meta data file that is used to write the output.
   */
  private final String metaDataPath;

  /**
   * Creates a new indexed CSV data sink. Computes the meta data based on the given graph.
   *
   * @param csvPath directory to write to
   * @param config  Gradoop Flink configuration
   */
  public TemporalIndexedCSVDataSink(String csvPath, GradoopFlinkConfig config) {
    this(csvPath, null, config);
  }

  /**
   * Creates an new indexed CSV data sink. Uses the specified meta data to write the CSV output.
   *
   * @param csvPath      directory to write CSV files to
   * @param metaDataPath path to meta data CSV file that is reused to prevent the creation of metadata
   * @param config       Gradoop Flink configuration
   */
  public TemporalIndexedCSVDataSink(String csvPath, String metaDataPath, GradoopFlinkConfig config) {
    super(csvPath, config);
    this.metaDataPath = metaDataPath;
  }

  @Override
  public void write(TemporalGraph temporalGraph) throws IOException {
    write(temporalGraph, false);
  }

  @Override
  public void write(TemporalGraphCollection temporalGraphCollection) throws IOException {
    write(temporalGraphCollection, false);
  }

  @Override
  public void write(TemporalGraph temporalGraph, boolean overwrite) throws IOException {
    write(temporalGraph.getCollectionFactory().fromGraph(temporalGraph), overwrite);
  }

  @Override
  public void write(TemporalGraphCollection temporalGraphCollection, boolean overwrite) throws IOException {
    WriteMode writeMode = overwrite ?
      WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE;

    DataSet<Tuple3<String, String, String>> metaData;
    CSVMetaDataSource source = new CSVMetaDataSource();
    if (isMetadataReusable()) {
      metaData = source.readDistributed(metaDataPath, getConfig());
    } else {
      metaData = source.tuplesFromCollection(temporalGraphCollection);
    }

    DataSet<TemporalCSVGraphHead> csvGraphHeads = temporalGraphCollection.getGraphHeads()
      .map(new TemporalGraphHeadToTemporalCSVGraphHead())
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<TemporalCSVVertex> csvVertices = temporalGraphCollection.getVertices()
      .map(new TemporalVertexToTemporalCSVVertex())
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<TemporalCSVEdge> csvEdges = temporalGraphCollection.getEdges()
      .map(new TemporalEdgeToTemporalCSVEdge())
      .withBroadcastSet(metaData, BC_METADATA);

    if (!getMetaDataPath().equals(metaDataPath) || !isMetadataReusable()) {
      new CSVMetaDataSink().writeDistributed(getMetaDataPath(), metaData, writeMode);
    }

    IndexedCSVFileFormat<TemporalCSVGraphHead> graphHeadFormat = new IndexedCSVFileFormat<>(
      new Path(getGraphHeadPath()),
      CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER);

    graphHeadFormat.setWriteMode(writeMode);
    csvGraphHeads.output(graphHeadFormat);

    IndexedCSVFileFormat<TemporalCSVVertex> vertexFormat = new IndexedCSVFileFormat<>(
      new Path(getVertexPath()),
      CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER);

    vertexFormat.setWriteMode(writeMode);
    csvVertices.output(vertexFormat);

    IndexedCSVFileFormat<TemporalCSVEdge> edgeFormat = new IndexedCSVFileFormat<>(
      new Path(getEdgePath()),
      CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER);

    edgeFormat.setWriteMode(writeMode);
    csvEdges.output(edgeFormat);
  }

  /**
   * Returns true, if the meta data shall be reused.
   *
   * @return true, iff reuse is possible
   */
  private boolean isMetadataReusable() {
    return this.metaDataPath != null && !this.metaDataPath.isEmpty();
  }
}
