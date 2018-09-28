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
package org.gradoop.flink.io.impl.deprecated.logicalgraphcsv;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.functions.ElementToPropertyMetaData;
import org.gradoop.flink.io.impl.csv.functions.ReducePropertyMetaData;
import org.gradoop.flink.io.impl.csv.metadata.MetaDataParser;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.util.Objects;

/**
 * Base class for CSV data source and data sink.
 */
public abstract class LogicalGraphCSVBase {
  /**
   * Broadcast set identifier for meta data.
   */
  public static final String BC_METADATA = "metadata";
  /**
   * File ending for CSV files.
   */
  private static final String CSV_FILE_SUFFIX = ".csv";
  /**
   * CSV file for vertices.
   */
  private static final String VERTEX_FILE = "vertices" + CSV_FILE_SUFFIX;
  /**
   * Path for indexed vertices
   */
  private static final String VERTEX_PATH = "vertices";
  /**
   * Path for indexed edges
   */
  private static final String EDGE_PATH = "edges";
  /**
   * CSV file for edges.
   */
  private static final String EDGE_FILE = "edges" + CSV_FILE_SUFFIX;
  /**
   * CSV file for meta data.
   */
  private static final String METADATA_FILE = "metadata" + CSV_FILE_SUFFIX;
  /**
   * Root directory containing the CSV and metadata files.
   */
  private final String csvRoot;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Constructor.
   *
   * @param csvPath directory to the CSV files
   * @param config Gradoop Flink configuration
   */
  protected LogicalGraphCSVBase(String csvPath, GradoopFlinkConfig config) {
    Objects.requireNonNull(csvPath);
    Objects.requireNonNull(config);
    this.csvRoot = csvPath.endsWith(File.separator) ? csvPath : csvPath + File.separator;
    this.config = config;
  }

  protected String getVertexCSVPath() {
    return csvRoot + VERTEX_FILE;
  }

  protected String getVertexPath() {
    return csvRoot + VERTEX_PATH;
  }

  protected String getEdgePath() {
    return csvRoot + EDGE_PATH;
  }

  /**
   * Returns the path to the vertex file containing only vertices with the specified label.
   *
   * @param label vertex label
   * @return path to csv file
   */
  protected String getVertexCSVPath(String label) {
    Objects.requireNonNull(label);
    return csvRoot +
      VERTEX_PATH +
      CSVConstants.DIRECTORY_SEPARATOR +
      label +
      CSVConstants.DIRECTORY_SEPARATOR +
      CSVConstants.SIMPLE_FILE;
  }

  protected String getEdgeCSVPath() {
    return csvRoot + EDGE_FILE;
  }

  /**
   * Returns the path to the edge file containing only edges with the specified label.
   *
   * @param label edge label
   * @return path to csv file
   */
  protected String getEdgeCSVPath(String label) {
    Objects.requireNonNull(label);
    return csvRoot +
      EDGE_PATH +
      CSVConstants.DIRECTORY_SEPARATOR +
      label +
      CSVConstants.DIRECTORY_SEPARATOR +
      CSVConstants.SIMPLE_FILE;
  }

  protected String getMetaDataPath() {
    return csvRoot + METADATA_FILE;
  }

  protected GradoopFlinkConfig getConfig() {
    return config;
  }

  /**
   * Creates the meta data for the given graph.
   *
   * @param graph logical graph
   * @return meta data information
   */
  protected DataSet<Tuple3<String, String, String>> createMetaData(LogicalGraph graph) {
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
  protected <E extends Element> DataSet<Tuple3<String, String, String>> createMetaData(
    DataSet<E> elements) {
    return elements
      .map(new ElementToPropertyMetaData<>())
      .groupBy(1)
      .reduce(new ReducePropertyMetaData())
      .map(tuple -> Tuple3.of(tuple.f0, tuple.f1, MetaDataParser.getPropertiesMetaData(tuple.f2)))
      .returns(new TupleTypeInfo<>(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO))
      .withForwardedFields("f0", "f1");
  }
}
