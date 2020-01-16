/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.io.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
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

/**
 * A data sink storing graphs and graph collections as CSV files.
 */
public class TemporalCSVDataSink extends CSVDataSink implements TemporalDataSink {

  /**
   * Initialize this data sink.
   *
   * @param csvPath The output path.
   * @param config  The Gradoop configuration.
   */
  public TemporalCSVDataSink(String csvPath, GradoopFlinkConfig config) {
    super(csvPath, config);
  }

  /**
   * Initialize this data sink with existing metadata.
   *
   * @param csvPath      The output path.
   * @param metaDataPath The metadata path.
   * @param config       The Gradoop configuration.
   */
  public TemporalCSVDataSink(String csvPath, String metaDataPath, GradoopFlinkConfig config) {
    super(csvPath, metaDataPath, config);
  }

  @Override
  public void write(TemporalGraph temporalGraph) {
    write(temporalGraph, false);
  }

  @Override
  public void write(TemporalGraphCollection temporalGraphCollection) {
    write(temporalGraphCollection, false);
  }

  @Override
  public void write(TemporalGraph temporalGraph, boolean overwrite) {
    write(temporalGraph.getCollectionFactory().fromGraph(temporalGraph), overwrite);
  }

  @Override
  public void write(TemporalGraphCollection temporalGraphCollection, boolean overwrite) {
    FileSystem.WriteMode writeMode = overwrite ?
      FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE;
    DataSet<Tuple3<String, String, String>> metaData;
    CSVMetaDataSource source = new CSVMetaDataSource();

    if (!reuseMetadata()) {
      metaData = source.tuplesFromCollection(temporalGraphCollection);
    } else {
      metaData = source.readDistributed(metaDataPath, getConfig());
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

    // Write metadata only if the path is not the same or reuseMetadata is false.
    if (!getMetaDataPath().equals(metaDataPath) || !reuseMetadata()) {
      new CSVMetaDataSink().writeDistributed(getMetaDataPath(), metaData, writeMode);
    }

    csvGraphHeads.writeAsCsv(getGraphHeadCSVPath(), CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER, writeMode);

    csvVertices.writeAsCsv(getVertexCSVPath(), CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER, writeMode);

    csvEdges.writeAsCsv(getEdgeCSVPath(), CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER, writeMode);
  }
}
