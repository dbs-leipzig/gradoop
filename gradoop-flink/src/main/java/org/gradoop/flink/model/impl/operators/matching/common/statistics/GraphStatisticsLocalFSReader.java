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

    Path statisticsDir = Paths.get(inputPath);
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
