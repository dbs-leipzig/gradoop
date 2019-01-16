/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.gradoop.flink.io.impl.csv.functions.StringEscaper;
import org.gradoop.flink.io.impl.csv.indexed.functions.MultipleFileOutputFormat;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.util.Objects;

/**
 * Base class for CSV data source and data sink.
 */
public abstract class CSVBase {
  /**
   * Broadcast set identifier for meta data.
   */
  public static final String BC_METADATA = "metadata";
  /**
   * File ending for CSV files.
   */
  private static final String CSV_FILE_SUFFIX = ".csv";
  /**
   * Path for indexed vertices
   */
  private static final String VERTEX_PATH = "vertices";
  /**
   * CSV file for vertices.
   */
  private static final String VERTEX_FILE = "vertices" + CSV_FILE_SUFFIX;
  /**
   * Path for indexed graph heads.
   */
  private static final String GRAPH_HEAD_PATH = "graphs";
  /**
   * CSV file containing the graph heads.
   */
  private static final String GRAPH_HEAD_FILE = "graphs" + CSV_FILE_SUFFIX;
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
   * @param config  Gradoop Flink configuration
   */
  protected CSVBase(String csvPath, GradoopFlinkConfig config) {
    Objects.requireNonNull(csvPath);
    Objects.requireNonNull(config);
    this.csvRoot = csvPath.endsWith(File.separator) ? csvPath : csvPath + File.separator;
    this.config = config;
  }

  /**
   * Returns the path to the graph head directory.
   *
   * @return graph head path
   */
  protected String getGraphHeadPath() {
    return csvRoot + GRAPH_HEAD_PATH;
  }

  /**
   * Returns the path to the vertex directory.
   *
   * @return vertex path
   */
  protected String getVertexPath() {
    return csvRoot + VERTEX_PATH;
  }

  /**
   * Returns the path to the edge directory.
   *
   * @return edge path
   */
  protected String getEdgePath() {
    return csvRoot + EDGE_PATH;
  }

  /**
   * Returns the path to the graph head file.
   *
   * @return graph head file path
   */
  protected String getGraphHeadCSVPath() {
    return csvRoot + GRAPH_HEAD_FILE;
  }

  /**
   * Returns the path to the vertex file.
   *
   * @return vertex file path
   */
  protected String getVertexCSVPath() {
    return csvRoot + VERTEX_FILE;
  }

  /**
   * Returns the path to the edge file.
   *
   * @return edge file path
   */
  protected String getEdgeCSVPath() {
    return csvRoot + EDGE_FILE;
  }

  /**
   * Returns the path to the graph head file containing only graph heads with the specified label.
   *
   * @param label graph head label
   * @return path to csv file
   */
  protected String getGraphHeadCSVPath(String label) {
    return getElementCSVPath(label, getGraphHeadPath());
  }

  /**
   * Returns the path to the vertex file containing only vertices with the specified label.
   *
   * @param label vertex label
   * @return path to csv file
   */
  protected String getVertexCSVPath(String label) {
    return getElementCSVPath(label, getVertexPath());
  }

  /**
   * Returns the path to the edge file containing only edges with the specified label.
   *
   * @param label edge label
   * @return path to csv file
   */
  protected String getEdgeCSVPath(String label) {
    return getElementCSVPath(label, getEdgePath());
  }

  /**
   * Returns the path to the element file containing only elements with the specified label.
   *
   * @param label       element label
   * @param elementPath path of the element (e.g. "edge")
   * @return path to csv file
   */
  private String getElementCSVPath(String label, String elementPath) {
    Objects.requireNonNull(label);
    if (label.isEmpty()) {
      label = CSVConstants.DEFAULT_DIRECTORY;
    } else {
      label = MultipleFileOutputFormat
        .cleanFilename(StringEscaper.escape(label, CSVConstants.ESCAPED_CHARACTERS));
    }
    return elementPath +
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
}
