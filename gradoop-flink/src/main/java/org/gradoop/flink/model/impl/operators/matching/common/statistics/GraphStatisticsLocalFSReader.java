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
public class GraphStatisticsLocalFSReader {
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
    long vertexCount = GraphStatisticsReader.readSingleValue(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_EDGE_COUNT));
    long edgeCount = GraphStatisticsReader.readSingleValue(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_VERTEX_COUNT_BY_LABEL));
    Map<String, Long> vertexCountByLabel = GraphStatisticsReader.readKeyValueMap(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_EDGE_COUNT_BY_LABEL));
    Map<String, Long> edgeCountByLabel = GraphStatisticsReader.readKeyValueMap(Files.lines(p, charset));

    p = statisticsDir
      .resolve(Paths.get(GraphStatisticsReader.FILE_EDGE_COUNT_BY_SOURCE_VERTEX_AND_EDGE_LABEL));
    Map<String, Map<String, Long>> edgeCountBySourceVertexAndEdgeLabel = GraphStatisticsReader
      .readNestedKeyValueMap(Files.lines(p, charset));

    p = statisticsDir
      .resolve(Paths.get(GraphStatisticsReader.FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL));
    Map<String, Map<String, Long>> edgeCountByTargetVertexAndEdgeLabel = GraphStatisticsReader
      .readNestedKeyValueMap(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT));
    long distinctSourceVertexCount = GraphStatisticsReader.readSingleValue(Files.lines(p, charset));

    p = statisticsDir.resolve(Paths.get(GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT));
    long distinctTargetVertexCount = GraphStatisticsReader.readSingleValue(Files.lines(p, charset));

    p = statisticsDir
      .resolve(Paths.get(GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT_BY_EDGE_LABEL));
    Map<String, Long> distinctSourceVertexCountByEdgeLabel = GraphStatisticsReader
      .readKeyValueMap(Files.lines(p, charset));

    p = statisticsDir
      .resolve(Paths.get(GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL));
    Map<String, Long> distinctTargetVertexCountByEdgeLabel = GraphStatisticsReader
      .readKeyValueMap(Files.lines(p, charset));

    return new GraphStatistics(vertexCount, edgeCount, vertexCountByLabel, edgeCountByLabel,
      edgeCountBySourceVertexAndEdgeLabel, edgeCountByTargetVertexAndEdgeLabel,
      distinctSourceVertexCount, distinctTargetVertexCount,
      distinctSourceVertexCountByEdgeLabel, distinctTargetVertexCountByEdgeLabel);
  }

}
