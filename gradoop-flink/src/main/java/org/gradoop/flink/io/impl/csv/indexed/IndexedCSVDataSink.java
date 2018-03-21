/**
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
package org.gradoop.flink.io.impl.csv.indexed;

import java.io.IOException;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.csv.CSVBase;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.functions.EdgeToCSVEdge;
import org.gradoop.flink.io.impl.csv.functions.ElementToPropertyMetaData;
import org.gradoop.flink.io.impl.csv.functions.ReducePropertyMetaData;
import org.gradoop.flink.io.impl.csv.functions.VertexToCSVVertex;
import org.gradoop.flink.io.impl.csv.indexed.functions.IndexedCSVFileFormat;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;
import org.gradoop.flink.io.impl.csv.metadata.MetaDataParser;
import org.gradoop.flink.io.impl.csv.tuples.CSVEdge;
import org.gradoop.flink.io.impl.csv.tuples.CSVVertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A graph data sink for CSV files indexed by label.
 */
public class IndexedCSVDataSink extends CSVBase implements DataSink {

  /**
   * Path to meta data file that is used to write the output.
   */
  private final String metaDataPath;

  /**
   * Creates a new indexed CSV data sink. Computes the meta data based on the given graph.
   *
   * @param csvPath directory to write to
   * @param config Gradoop Flink configuration
   * @throws IOException
   */
  public IndexedCSVDataSink(String csvPath, GradoopFlinkConfig config) throws IOException {
    this(csvPath, null, config);
  }

  /**
   * Creates an new indexed CSV data sink. Uses the specified meta data to write the CSV output.
   *
   * @param csvPath directory to write CSV files to
   * @param metaDataPath path to meta data CSV file
   * @param config Gradoop Flink configuration
   */
  public IndexedCSVDataSink(String csvPath, String metaDataPath, GradoopFlinkConfig config)
      throws IOException {
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
    FileSystem.WriteMode writeMode = overwrite ?
        FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE;

    DataSet<Tuple2<String, String>> metaData;
    if (!reuseMetadata()) {
      metaData = createMetaData(logicalGraph);
    } else {
      metaData = MetaData.fromFile(metaDataPath, getConfig());
    }

    DataSet<CSVVertex> csvVertices = logicalGraph.getVertices()
      .map(new VertexToCSVVertex())
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<CSVEdge> csvEdges = logicalGraph.getEdges()
        .map(new EdgeToCSVEdge())
        .withBroadcastSet(metaData, BC_METADATA);

    if (!getMetaDataPath().equals(metaDataPath) || !reuseMethadata()) {
      metaData.writeAsCsv(getMetaDataPath(), CSVConstants.ROW_DELIMITER,
        CSVConstants.TOKEN_DELIMITER, writeMode).setParallelism(1);
    }
    csvVertices.output(internalWriteAsIndexedCsv(csvVertices, new Path(getVertexCSVPath()),
        CSVConstants.ROW_DELIMITER, CSVConstants.TOKEN_DELIMITER, writeMode));

    csvEdges.output(internalWriteAsIndexedCsv(csvEdges, new Path(getEdgeCSVPath()),
        CSVConstants.ROW_DELIMITER, CSVConstants.TOKEN_DELIMITER, writeMode));
  }

  /**
   * Writes a {@link Tuple} DataSet as CSV file(s) to the specified location with the specified field and line delimiters.<br>
   * <b>Note: Only a Tuple DataSet can written as a CSV file.</b><br>
   * For each Tuple field the result of {@link Object#toString()} is written.
   *
   * @param dataSet The Tuple that should be writen in the CSV file.
   * @param filePath The path pointing to the location the CSV file is written to.
   * @param rowDelimiter The row delimiter to separate Tuples.
   * @param fieldDelimiter The field delimiter to separate Tuple fields.
   * @param wm The behavior regarding existing files. Options are NO_OVERWRITE and OVERWRITE.
   * @param <X> Tuple for indexed CSV file format.
   * @return An indexed CSV file format for the tuple.
   *
   * references to: {@link org.apache.flink.api.java.DataSet#writeAsCsv(String, String, String, WriteMode)}
   */
  @SuppressWarnings("unchecked")
  private <X extends Tuple> IndexedCSVFileFormat<X> internalWriteAsIndexedCsv(
      DataSet dataSet, Path filePath, String rowDelimiter, String fieldDelimiter,
      WriteMode wm) {
    Preconditions.checkArgument(dataSet.getType().isTupleType(),
      "The writeAsCsv() method can only be used on data sets of tuples.");
    IndexedCSVFileFormat<X> of = new IndexedCSVFileFormat<>(filePath, rowDelimiter, fieldDelimiter);
    of.setWriteMode(wm);
    return of;
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) throws IOException {
    // TODO Auto-generated method stub
  }

  /**
   * Returns true, if the meta data shall be reused.
   *
   * @return true, iff reuse is possible
   */
  private boolean reuseMetadata() {
    return this.metaDataPath != null && !this.metaDataPath.isEmpty();
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
      .combineGroup(new ReducePropertyMetaData())
      .groupBy(0)
      .reduceGroup(new ReducePropertyMetaData())
      .map(tuple -> Tuple2.of(tuple.f0, MetaDataParser.getPropertiesMetaData(tuple.f1)))
      .returns(new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
      .withForwardedFields("f0");
  }
}
