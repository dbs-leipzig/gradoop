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
      GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTY_VALUES_BY_LABEL_AND_PROPERTY_KEY);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctPropertyValuesByEdgeLabelAndPropertyName = readNestedKeyValueMap(br.lines());
    }

    p = new Path(root,
      GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTY_VALUES_BY_LABEL_AND_PROPERTY_KEY);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctPropertyValuesByVertexLabelAndPropertyName = readNestedKeyValueMap(br.lines());
    }

    p = new Path(root,
      GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTY_VALUES_BY_PROPERTY_KEY);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), charset))) {
      distinctEdgePropertyValuesByPropertyName = readKeyValueMap(br.lines());
    }

    p = new Path(root,
      GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTY_VALUES_BY_PROPERTY_KEY);
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
