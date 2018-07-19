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
package org.gradoop.flink.model.impl.operators.matching.common.statistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Reads {@link GraphStatistics} from dedicated files in HDFS.
 */
public class GraphStatisticsHDFSReader extends GraphStatisticsReader {
  /**
   * Reads statistics from files contains in the specified directory and creates a
   * {@link GraphStatistics} object from them.
   *
   * The method expects all files to be present and formatted according to the docs.
   *
   * @param inputPath path to directory containing statistics files
   * @param configuration Hadoop configuration
   * @return graph statistics
   * @throws IOException if an I/O error occurs opening the files
   */
  public static GraphStatistics read(String inputPath, Configuration configuration)
      throws IOException {
    FileSystem fs = FileSystem.get(configuration);
    Path root = new Path(inputPath);
    Charset charset = Charset.forName("UTF-8");

    long vertexCount;
    long edgeCount;
    Map<String, Long> vertexCountByLabel;
    Map<String, Long> edgeCountByLabel;
    Map<String, Map<String, Long>> edgeCountBySourceVertexAndEdgeLabel;
    Map<String, Map<String, Long>> edgeCountByTargetVertexAndEdgeLabel;
    long distinctSourceVertexCount;
    long distinctTargetVertexCount;
    Map<String, Long> distinctSourceVertexCountByEdgeLabel;
    Map<String, Long> distinctTargetVertexCountByEdgeLabel;
    Map<String, Map<String, Long>> distinctPropertyValuesByEdgeLabelAndPropertyName;
    Map<String, Map<String, Long>> distinctPropertyValuesByVertexLabelAndPropertyName;
    Map<String, Long> distinctEdgePropertyValuesByPropertyName;
    Map<String, Long> distinctVertexPropertyValuesByPropertyName;

    Path p = new Path(root, GraphStatisticsReader.FILE_VERTEX_COUNT);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      vertexCount = readSingleValue(br.lines());
    }

    p = new Path(root, GraphStatisticsReader.FILE_EDGE_COUNT);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      edgeCount = readSingleValue(br.lines());
    }

    p = new Path(root, GraphStatisticsReader.FILE_VERTEX_COUNT_BY_LABEL);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      vertexCountByLabel = readKeyValueMap(br.lines());
    }

    p = new Path(root, GraphStatisticsReader.FILE_EDGE_COUNT_BY_LABEL);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      edgeCountByLabel = readKeyValueMap(br.lines());
    }

    p = new Path(root, GraphStatisticsReader.FILE_EDGE_COUNT_BY_SOURCE_VERTEX_AND_EDGE_LABEL);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      edgeCountBySourceVertexAndEdgeLabel = readNestedKeyValueMap(br.lines());
    }

    p = new Path(root, GraphStatisticsReader.FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      edgeCountByTargetVertexAndEdgeLabel = readNestedKeyValueMap(br.lines());
    }

    p = new Path(root, GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctSourceVertexCount = readSingleValue(br.lines());
    }

    p = new Path(root, GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctTargetVertexCount = readSingleValue(br.lines());
    }

    p = new Path(root, GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT_BY_EDGE_LABEL);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctSourceVertexCountByEdgeLabel = readKeyValueMap(br.lines());
    }

    p = new Path(root, GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctTargetVertexCountByEdgeLabel = readKeyValueMap(br.lines());
    }

    p = new Path(root,
      GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES_BY_LABEL);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctPropertyValuesByEdgeLabelAndPropertyName = readNestedKeyValueMap(br.lines());
    }

    p = new Path(root,
      GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES_BY_LABEL);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctPropertyValuesByVertexLabelAndPropertyName = readNestedKeyValueMap(br.lines());
    }

    p = new Path(root,
      GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctEdgePropertyValuesByPropertyName = readKeyValueMap(br.lines());
    }

    p = new Path(root,
      GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctVertexPropertyValuesByPropertyName = readKeyValueMap(br.lines());
    }

    return new GraphStatistics(vertexCount, edgeCount, vertexCountByLabel, edgeCountByLabel,
      edgeCountBySourceVertexAndEdgeLabel, edgeCountByTargetVertexAndEdgeLabel,
      distinctSourceVertexCount, distinctTargetVertexCount, distinctSourceVertexCountByEdgeLabel,
      distinctTargetVertexCountByEdgeLabel, distinctPropertyValuesByEdgeLabelAndPropertyName,
      distinctPropertyValuesByVertexLabelAndPropertyName,
      distinctEdgePropertyValuesByPropertyName, distinctVertexPropertyValuesByPropertyName);
  }
}
