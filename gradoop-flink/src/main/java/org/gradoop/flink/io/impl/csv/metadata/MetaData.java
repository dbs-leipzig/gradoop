/**
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
package org.gradoop.flink.io.impl.csv.metadata;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Describes the data stored in the vertex and edge CSV files.
 */
public class MetaData {
  /**
   * Mapping between an element label and its associated property meta data.
   */
  private Map<String, List<PropertyMetaData>> metaData;

  /**
   * Constructor
   *
   * @param metaData meta data
   */
  MetaData(Map<String, List<PropertyMetaData>> metaData) {
    this.metaData = metaData;
  }

  /**
   * Reads the meta data from a specified file. Each line is spit into a (label, metadata) tuple
   * and put into a dataset. The latter can be used to broadcast the metadata to the mappers.
   *
   * @param path path to metadata csv file
   * @param config gradoop configuration
   * @return (label, metadata) tuple dataset
   */
  public static DataSet<Tuple2<String, String>> fromFile(String path, GradoopFlinkConfig config) {
    return config.getExecutionEnvironment()
      .readTextFile(path)
      .map(line -> {
          String[] tokens = line.split(CSVConstants.TOKEN_DELIMITER, 2);
          return Tuple2.of(tokens[0], tokens[1]);
        })
      .returns(new TypeHint<Tuple2<String, String>>() { });
  }

  /**
   * Reads the meta data from a specified csv file. The file can be either located in a local file
   * system or in HDFS.
   *
   * @param path path to metadata csv file
   * @param hdfsConfig file system configuration
   * @return meta data
   * @throws IOException
   */
  public static MetaData fromFile(String path, Configuration hdfsConfig) throws IOException {
    FileSystem fs = FileSystem.get(hdfsConfig);
    Path file = new Path(path);
    Charset charset = Charset.forName("UTF-8");

    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file), charset))) {
      return MetaDataParser.create(br.lines()
        .map(line -> line.split(CSVConstants.TOKEN_DELIMITER, 2))
        .map(tokens -> Tuple2.of(tokens[0], tokens[1]))
        .collect(Collectors.toList()));
    }
  }

  /**
   * Returns the vertex labels available in the meta data.
   *
   * @return vertex labels
   */
  public Set<String> getVertexLabels() {
    return metaData.keySet().stream()
      .filter(l -> Character.isUpperCase(l.charAt(0)))
      .collect(Collectors.toSet());
  }

  /**
   * Returns the edge labels available in the meta data.
   *
   * @return edge labels
   */
  public Set<String> getEdgeLabels() {
    return metaData.keySet().stream()
      .filter(l -> Character.isLowerCase(l.charAt(0)))
      .collect(Collectors.toSet());
  }

  /**
   * Returns the property meta data associated with the specified label.
   *
   * @param label element label
   * @return property meta data for the element
   */
  public List<PropertyMetaData> getPropertyMetaData(String label) {
    return metaData.get(label);
  }
}
