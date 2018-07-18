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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Reads {@link GraphStatistics} from dedicated files in the local file system.
 */
public class GraphStatisticsLocalFSReader extends GraphStatisticsReader {
  /**
   * Reads statistics from files contains in the specified directory and creates a
   * {@link GraphStatistics} object from them.
   *
   * The method expects all files to be present and formatted according to the docs.
   *
   * @param inputPath path to directory containing statistics files
   * @return graph statistics
   * @throws IOException if an I/O error occurs opening the files
   */
  public static GraphStatistics read(String inputPath) throws IOException {

    Path statisticsDir = Paths.get(new File(inputPath).getAbsolutePath());
    Charset charset = Charset.forName("UTF-8");

    Path p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_VERTEX_COUNT));
    long vertexCount = readSingleValue(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_EDGE_COUNT));
    long edgeCount = readSingleValue(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_VERTEX_COUNT_BY_LABEL));
    Map<String, Long> vertexCountByLabel = readKeyValueMap(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_EDGE_COUNT_BY_LABEL));
    Map<String, Long> edgeCountByLabel = readKeyValueMap(Files.lines(p, charset));

    p = statisticsDir
      .resolve(Paths.get(GraphStatisticsReader.FILE_EDGE_COUNT_BY_SOURCE_VERTEX_AND_EDGE_LABEL));
    Map<String, Map<String, Long>> edgeCountBySourceVertexAndEdgeLabel =
      readNestedKeyValueMap(Files.lines(p, charset));

    p = statisticsDir
      .resolve(Paths.get(GraphStatisticsReader.FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL));
    Map<String, Map<String, Long>> edgeCountByTargetVertexAndEdgeLabel =
      readNestedKeyValueMap(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT));
    long distinctSourceVertexCount = readSingleValue(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT));
    long distinctTargetVertexCount = readSingleValue(Files.lines(p, charset));

    p = statisticsDir
      .resolve(Paths.get(GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT_BY_EDGE_LABEL));
    Map<String, Long> distSourceVertexCountByEdgeLabel = readKeyValueMap(Files.lines(p, charset));

    p = statisticsDir
      .resolve(Paths.get(GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL));
    Map<String, Long> distTargetVertexCountByEdgeLabel = readKeyValueMap(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(
        GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES_BY_LABEL));
    Map<String, Map<String, Long>> distinctPropertyValuesByEdgeLabelAndPropertyName =
      readNestedKeyValueMap(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(
      GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES_BY_LABEL));
    Map<String, Map<String, Long>> distinctPropertyValuesByVertexLabelAndPropertyName =
      readNestedKeyValueMap(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(
      GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES));
    Map<String, Long> distinctEdgePropertyValuesByPropertyName =
      readKeyValueMap(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(
      GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES));
    Map<String, Long> distinctVertexPropertyValuesByPropertyName =
      readKeyValueMap(Files.lines(p, charset));

    return new GraphStatistics(vertexCount, edgeCount, vertexCountByLabel, edgeCountByLabel,
      edgeCountBySourceVertexAndEdgeLabel, edgeCountByTargetVertexAndEdgeLabel,
      distinctSourceVertexCount, distinctTargetVertexCount,
      distSourceVertexCountByEdgeLabel, distTargetVertexCountByEdgeLabel,
      distinctPropertyValuesByEdgeLabelAndPropertyName,
      distinctPropertyValuesByVertexLabelAndPropertyName,
      distinctEdgePropertyValuesByPropertyName,
      distinctVertexPropertyValuesByPropertyName);
  }

}
