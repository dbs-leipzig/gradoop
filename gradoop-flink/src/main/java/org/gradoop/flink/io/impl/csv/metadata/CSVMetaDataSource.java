/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gradoop.common.model.impl.metadata.PropertyMetaData;
import org.gradoop.flink.io.api.metadata.MetaDataSource;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.functions.StringEscaper;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of a meta data factory that reads from csv files.
 */
public class CSVMetaDataSource implements MetaDataSource<CSVMetaData> {

  @Override
  public CSVMetaData fromTuples(List<Tuple3<String, String, String>> metaDataStrings) {
    Map<String, List<PropertyMetaData>> graphMetaDataMap = new HashMap<>(metaDataStrings.size());
    Map<String, List<PropertyMetaData>> vertexMetaDataMap = new HashMap<>(metaDataStrings.size());
    Map<String, List<PropertyMetaData>> edgeMetaDataMap = new HashMap<>(metaDataStrings.size());

    for (Tuple3<String, String, String> tuple : metaDataStrings) {
      List<PropertyMetaData> propertyMetaDataList;
      if (tuple.f2.length() > 0) {
        String[] propertyStrings = StringEscaper.split(
          tuple.f2, PropertyMetaData.PROPERTY_DELIMITER);
        propertyMetaDataList = new ArrayList<>(propertyStrings.length);
        for (String propertyMetaData : propertyStrings) {
          String[] propertyMetaDataTokens = StringEscaper.split(
            propertyMetaData, PropertyMetaData.PROPERTY_TOKEN_DELIMITER, 2);
          propertyMetaDataList.add(new PropertyMetaData(
            StringEscaper.unescape(propertyMetaDataTokens[0]),
            propertyMetaDataTokens[1],
            CSVMetaDataParser.getPropertyValueParser(propertyMetaDataTokens[1])));
        }
      } else {
        propertyMetaDataList = new ArrayList<>(0);
      }
      String label = StringEscaper.unescape(tuple.f1);
      switch (tuple.f0) {
      case GRAPH_TYPE:
        graphMetaDataMap.put(label, propertyMetaDataList);
        break;
      case VERTEX_TYPE:
        vertexMetaDataMap.put(label, propertyMetaDataList);
        break;
      case EDGE_TYPE:
        edgeMetaDataMap.put(label, propertyMetaDataList);
        break;
      default:
        throw new IllegalArgumentException("Type " + tuple.f0 + " is not a valid epgm type " +
          "string. Valid strings are g, v and e.");
      }
    }

    return new CSVMetaData(graphMetaDataMap, vertexMetaDataMap, edgeMetaDataMap);
  }

  @Override
  public DataSet<Tuple3<String, String, String>> readDistributed(String path, GradoopFlinkConfig
    config) {
    return config.getExecutionEnvironment()
      .readTextFile(path)
      .map(line -> StringEscaper.split(line, CSVConstants.TOKEN_DELIMITER, 3))
      .map(tokens -> Tuple3.of(tokens[0], tokens[1], tokens[2]))
      .returns(new TypeHint<Tuple3<String, String, String>>() {
      });
  }

  @Override
  public CSVMetaData readLocal(String path, Configuration hdfsConfig) throws IOException {
    FileSystem fs = FileSystem.get(hdfsConfig);
    Path file = new Path(path);
    Charset charset = Charset.forName("UTF-8");

    if (!fs.exists(file)) {
      return null;
    } else {

      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file), charset))) {
        return fromTuples(br.lines()
          .map(line -> StringEscaper.split(line, CSVConstants.TOKEN_DELIMITER, 3))
          .map(tokens -> Tuple3.of(tokens[0], tokens[1], tokens[2]))
          .collect(Collectors.toList()));
      }
    }
  }
}
