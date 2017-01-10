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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reads {@link GraphStatistics} from dedicated files in the local file system.
 */
public class GraphStatisticsLocalFSReader {
  /**
   * Separates tokens in a single line
   */
  public static final String TOKEN_SEPARATOR = ",";

  //------------------------------------------------------------------------------------------------
  // Default file names
  //------------------------------------------------------------------------------------------------

  /**
   * Single line containing the total vertex count, e.g.
   *
   * BOF
   * 23
   * EOF
   */
  public static final String FILE_VERTEX_COUNT = "vertex_count";
  /**
   * Single line containing the total edge count, e.g.
   *
   * BOF
   * 42
   * EOF
   */
  public static final String FILE_EDGE_COUNT = "edge_count";
  /**
   * Each line contains the label and its count, e.g.
   *
   * BOF
   * Person,12
   * University,11
   * EOF
   */
  public static final String FILE_VERTEX_COUNT_BY_LABEL = "vertex_count_by_label";
  /**
   * Each line contains the label and its count, e.g.
   *
   * BOF
   * knows,30
   * studyAt,12
   * EOF
   */
  public static final String FILE_EDGE_COUNT_BY_LABEL = "edge_count_by_label";
  /**
   *  Each line contains the source vertex label, the edge label and the frequency., e.g.
   *
   *  BOF
   *  Person,knows,30
   *  Person,studyAt,12
   *  EOF
   */
  public static final String FILE_EDGE_COUNT_BY_SOURCE_VERTEX_AND_EDGE_LABEL =
    "edge_count_by_source_vertex_and_edge_label";
  /**
   * Each line contains the target vertex label, the edge label and the frequency, e.g.
   *
   * BOF
   * Person,knows,30
   * University,studyAt,12
   * EOF
   */
  public static final String FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL =
    "edge_count_by_target_vertex_and_edge_label";
  /**
   * One line containing the number of distinct source vertices, e.g.
   *
   * BOF
   * 23
   * EOF
   */
  public static final String FILE_DISTINCT_SOURCE_VERTEX_COUNT = "distinct_source_vertex_count";
  /**
   * One line containing the number of distinct target vertices, e.g.
   *
   * BOF
   * 42
   * EOF
   */
  public static final String FILE_DISTINCT_TARGET_VERTEX_COUNT = "distinct_target_vertex_count";
  /**
   * Each line contains the edge label and the number of distinct source ids, e.g.
   *
   * BOF
   * knows,10
   * studyAt,12
   * EOF
   */
  public static final String FILE_DISTINCT_SOURCE_VERTEX_COUNT_BY_EDGE_LABEL =
    "distinct_source_vertex_count_by_edge_label";
  /**
   * Each line contains the edge label and the number of distinct target ids, e.g.
   *
   * BOF
   * knows,10
   * studyAt,12
   * EOF
   */
  public static final String FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL =
    "distinct_target_vertex_count_by_edge_label";

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
  public static GraphStatistics readFromLocalFS(String inputPath) throws IOException {

    Path p = Paths.get(inputPath);

    long vertexCount = readSingleValue(p.resolve(Paths.get(FILE_VERTEX_COUNT)));
    long edgeCount = readSingleValue(p.resolve(Paths.get(FILE_EDGE_COUNT)));

    Map<String, Long> vertexCountByLabel = readKeyValueMap(
      p.resolve(Paths.get(FILE_VERTEX_COUNT_BY_LABEL)));
    Map<String, Long> edgeCountByLabel = readKeyValueMap(
      p.resolve(Paths.get(FILE_EDGE_COUNT_BY_LABEL)));

    Map<String, Map<String, Long>> edgeCountBySourceVertexAndEdgeLabel = readNestedKeyValueMap(
      p.resolve(Paths.get(FILE_EDGE_COUNT_BY_SOURCE_VERTEX_AND_EDGE_LABEL)));

    Map<String, Map<String, Long>> edgeCountByTargetVertexAndEdgeLabel = readNestedKeyValueMap(
      p.resolve(Paths.get(FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL)));

    long distinctSourceVertexCount = readSingleValue(
      p.resolve(Paths.get(FILE_DISTINCT_SOURCE_VERTEX_COUNT)));
    long distinctTargetVertexCount = readSingleValue(
      p.resolve(Paths.get(FILE_DISTINCT_TARGET_VERTEX_COUNT)));

    Map<String, Long> distinctSourceVertexCountByEdgeLabel = readKeyValueMap(
      p.resolve(Paths.get(FILE_DISTINCT_SOURCE_VERTEX_COUNT_BY_EDGE_LABEL)));
    Map<String, Long> distinctTargetVertexCountByEdgeLabel = readKeyValueMap(
      p.resolve(Paths.get(FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL)));

    return new GraphStatistics(vertexCount, edgeCount, vertexCountByLabel, edgeCountByLabel,
      edgeCountBySourceVertexAndEdgeLabel, edgeCountByTargetVertexAndEdgeLabel,
      distinctSourceVertexCount, distinctTargetVertexCount,
      distinctSourceVertexCountByEdgeLabel, distinctTargetVertexCountByEdgeLabel);
  }

  /**
   * Reads a single {@link Long} value from the specified file.
   *
   * @param p file to read
   * @return long value in first line of file
   * @throws IOException if an I/O error occurs opening the file
   */
  private static Long readSingleValue(Path p) throws IOException {
    return Files.lines(p)
      .map(Long::parseLong)
      .collect(Collectors.toList())
      .get(0);
  }

  /**
   * Reads a key value map from the specified file.
   *
   * @param p file to read
   * @return key value map
   * @throws IOException if an I/O error occurs opening the file
   */
  private static Map<String, Long> readKeyValueMap(Path p) throws IOException {
    return Files.lines(p)
      .map(s -> s.split(TOKEN_SEPARATOR))
      .collect(Collectors.toMap(tokens -> tokens[0], tokens -> Long.parseLong(tokens[1])));
  }

  /**
   * Reads a key value mapped from the given file grouped by the first token in each line.
   *
   * @param p file to read
   * @return nested key value map
   * @throws IOException if an I/O error occurs opening the file
   */
  private static Map<String, Map<String, Long>> readNestedKeyValueMap(Path p) throws IOException {

    final Map<String, Map<String, Long>> mapping = new HashMap<>();

    Files.lines(p)
      .map(line -> line.split(TOKEN_SEPARATOR))
      .forEach(tokens -> {
          String vertexLabel = tokens[0];
          String edgeLabel = tokens[1];
          Long edgeCount = Long.parseLong(tokens[2]);
          if (mapping.containsKey(vertexLabel)) {
            mapping.get(vertexLabel).put(edgeLabel, edgeCount);
          } else {
            Map<String, Long> value = new HashMap<>();
            value.put(edgeLabel, edgeCount);
            mapping.put(vertexLabel, value);
          }
        });

    return mapping;
  }
}
