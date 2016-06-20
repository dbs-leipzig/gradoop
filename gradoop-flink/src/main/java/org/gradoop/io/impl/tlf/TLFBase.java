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

package org.gradoop.io.impl.tlf;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.gradoop.io.impl.tlf.functions.TLFDictionaryStringToTuple;
import org.gradoop.io.impl.tlf.functions.TLFDictionaryTupleToMapGroupReducer;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Map;

/**
 * Base class for TLF data source and sink.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
abstract class TLFBase
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig<G, V, E> config;
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
   * True if tlfVertexDictionaryPath != ""
   */
  private boolean hasVertexDictionary;
  /**
   * True if tlfEdgeDictionaryPath != ""
   */
  private boolean hasEdgeDictionary;
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
    tlfEdgeDictionaryPath, GradoopFlinkConfig<G, V, E> config) {
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

    hasVertexDictionary = !tlfVertexDictionaryPath.equals("");
    hasEdgeDictionary = !tlfEdgeDictionaryPath.equals("");

    ExecutionEnvironment env = config.getExecutionEnvironment();
    if (hasVertexDictionary) {

      vertexDictionary = env
        .readHadoopFile(new TextInputFormat(), LongWritable.class, Text
          .class, getTLFVertexDictionaryPath())
        .map(new TLFDictionaryStringToTuple())
        .reduceGroup(new TLFDictionaryTupleToMapGroupReducer());
    }
    if (hasEdgeDictionary) {
      edgeDictionary = env
        .readHadoopFile(new TextInputFormat(), LongWritable.class, Text
          .class, getTLFEdgeDictionaryPath())
        .map(new TLFDictionaryStringToTuple())
        .reduceGroup(new TLFDictionaryTupleToMapGroupReducer());
    }
  }

  public GradoopFlinkConfig<G, V, E> getConfig() {
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
    return hasVertexDictionary;
  }

  /**
   * Returns true if there is an edge dictionary.
   *
   * @return true if there is an edge dictionary.
   */
  public boolean hasEdgeDictionary() {
    return hasEdgeDictionary;
  }

  public DataSet<Map<Integer, String>> getVertexDictionary() {
    return vertexDictionary;
  }

  public DataSet<Map<Integer, String>> getEdgeDictionary() {
    return edgeDictionary;
  }
}
