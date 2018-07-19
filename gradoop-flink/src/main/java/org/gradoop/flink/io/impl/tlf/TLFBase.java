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
package org.gradoop.flink.io.impl.tlf;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Map;

/**
 * Base class for TLF data source and sink.
 */
abstract class TLFBase {
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;
  /**
   * File to read/write TLF content to
   */
  private final String tlfPath;
  /**
   * File to read TLF vertex dictionary;
   */
  private final String tlfVertexDictionaryPath;
  /**
   * File to read TLF edge dictionary;
   */
  private final String tlfEdgeDictionaryPath;
  /**
   * Dataset containing one entry which is the vertex dictionary.
   */
  private DataSet<Map<Integer, String>> vertexDictionary;
  /**
   * Dataset containing ine entry which is the edge dictionary.
   */
  private DataSet<Map<Integer, String>> edgeDictionary;

  /**
   * Creates a new data source/sink. Paths can be local (file://) or HDFS
   * (hdfs://).
   *
   * @param tlfPath tlf data file
   * @param tlfVertexDictionaryPath tlf vertex dictionary file
   * @param tlfEdgeDictionaryPath tlf edge dictionary file
   * @param config Gradoop Flink configuration
   */
  TLFBase(String tlfPath, String tlfVertexDictionaryPath, String
    tlfEdgeDictionaryPath, GradoopFlinkConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    if (tlfPath == null) {
      throw new IllegalArgumentException("vertex file must not be null");
    }

    this.tlfPath = tlfPath;
    this.tlfVertexDictionaryPath = tlfVertexDictionaryPath;
    this.tlfEdgeDictionaryPath = tlfEdgeDictionaryPath;
    this.config = config;

  }

  public GradoopFlinkConfig getConfig() {
    return config;
  }

  public String getTLFPath() {
    return tlfPath;
  }

  public String getTLFVertexDictionaryPath() {
    return tlfVertexDictionaryPath;
  }

  public String getTLFEdgeDictionaryPath() {
    return tlfEdgeDictionaryPath;
  }

  /**
   * Returns true if there is a vertex dictionary.
   *
   * @return true if there is a vertex dictionary.
   */
  public boolean hasVertexDictionary() {
    return !tlfVertexDictionaryPath.equals("");
  }

  /**
   * Returns true if there is an edge dictionary.
   *
   * @return true if there is an edge dictionary.
   */
  public boolean hasEdgeDictionary() {
    return !tlfEdgeDictionaryPath.equals("");
  }

  public DataSet<Map<Integer, String>> getVertexDictionary() {
    return vertexDictionary;
  }

  public void setVertexDictionary(
    DataSet<Map<Integer, String>> vertexDictionary) {
    this.vertexDictionary = vertexDictionary;
  }

  public DataSet<Map<Integer, String>> getEdgeDictionary() {
    return edgeDictionary;
  }

  public void setEdgeDictionary(DataSet<Map<Integer, String>> edgeDictionary) {
    this.edgeDictionary = edgeDictionary;
  }
}
