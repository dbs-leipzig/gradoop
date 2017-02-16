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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for reading a {@link GraphStatistics} object from file system.
 */
public abstract class GraphStatisticsReader {
  /**
   * Separates tokens in a single line
   */
  public static final String TOKEN_SEPARATOR = ",";
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
   * Each line contains the edge label a property name and the number of distinct property
   * values for that pair, e.g.
   *
   * BOF
   * knows,since,73
   * connecting,isActive,2
   * EOF
   */
  public static final String FILE_DISTINCT_EDGE_PROPERTY_VALUES_BY_LABEL_AND_PROPERTY_KEY =
    "distinct_edge_property_values_by_label_and_property_name";

  /**
   * Each line contains the vertex label a property name and the number of distinct property
   * values for that pair, e.g.
   *
   * BOF
   * Person,age,100
   * City,name,25
   * EOF
   */
  public static final String FILE_DISTINCT_VERTEX_PROPERTY_VALUES_BY_LABEL_AND_PROPERTY_KEY =
    "distinct_vertex_property_values_by_label_and_property_name";

  /**
   * Each line contains the edge property name and the number of distinct property
   * values for that value
   *
   * BOF
   * since,73
   * isActive,2
   * EOF
   */
  public static final String FILE_DISTINCT_EDGE_PROPERTY_VALUES_BY_PROPERTY_KEY =
    "distinct_edge_property_values_by_property_name";

  /**
   * Each line contains the vertex property name and the number of distinct property
   * values for that value
   *
   * BOF
   * age,100
   * name,25
   * EOF
   */
  public static final String FILE_DISTINCT_VERTEX_PROPERTY_VALUES_BY_PROPERTY_KEY =
    "distinct_vertex_property_values_by_property_name";

  /**
   * Reads a single {@link Long} value from the specified file.
   *
   * @param lines stream of lines in the file
   * @return long value in first line of file
   * @throws IOException if an I/O error occurs opening the file
   */
  static Long readSingleValue(Stream<String> lines) throws IOException {
    return lines
      .map(Long::parseLong)
      .collect(Collectors.toList())
      .get(0);
  }

  /**
   * Reads a key value map from the specified file.
   *
   * @param lines stream of lines in the file
   * @return key value map
   * @throws IOException if an I/O error occurs opening the file
   */
  static Map<String, Long> readKeyValueMap(Stream<String> lines) throws IOException {
    return lines
      .map(s -> s.split(TOKEN_SEPARATOR))
      .collect(Collectors.toMap(tokens -> tokens[0], tokens -> Long.parseLong(tokens[1])));
  }

  /**
   * Reads a key value mapped from the given file grouped by the first token in each line.
   *
   * @param lines stream of lines in the file
   * @return nested key value map
   * @throws IOException if an I/O error occurs opening the file
   */
  static Map<String, Map<String, Long>> readNestedKeyValueMap(Stream<String> lines)
      throws IOException {

    final Map<String, Map<String, Long>> mapping = new HashMap<>();

    lines
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
