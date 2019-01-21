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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.gradoop.common.model.impl.metadata.PropertyMetaData;
import org.gradoop.flink.io.api.metadata.MetaDataSink;
import org.gradoop.flink.io.api.metadata.MetaDataSource;
import org.gradoop.flink.io.impl.csv.CSVConstants;

import java.io.IOException;
import java.util.List;

/**
 * Implementaion of a {@link MetaDataSink}
 */
public class CSVMetaDataSink implements MetaDataSink<CSVMetaData> {

  @Override
  public void writeDistributed(
    String metaDataPath,
    DataSet<Tuple3<String, String, String>> metaDataTuples,
    FileSystem.WriteMode writeMode) {
    metaDataTuples.writeAsCsv(metaDataPath, CSVConstants.ROW_DELIMITER,
      CSVConstants.TOKEN_DELIMITER, writeMode).setParallelism(1);

  }

  @Override
  public void writeLocal(
    String metaDataPath,
    CSVMetaData metaData,
    Configuration hdfsConfig,
    boolean overwrite) {
    try {
      org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(hdfsConfig);
      Path file = new Path(metaDataPath);
      if (fs.exists(file)) {
        if (!overwrite) {
          return;
        } else {
          fs.delete(file, false);
        }
      }
      FSDataOutputStream outputStream = fs.create(file);

      for (String graphLabel : metaData.getGraphLabels()) {
        outputStream.writeBytes(constructMetaDataString(
          MetaDataSource.GRAPH_TYPE,
          graphLabel,
          metaData.getGraphPropertyMetaData(graphLabel)));
        outputStream.writeBytes(CSVConstants.ROW_DELIMITER);
      }

      for (String vertexLabel : metaData.getVertexLabels()) {
        outputStream.writeBytes(constructMetaDataString(
          MetaDataSource.VERTEX_TYPE,
          vertexLabel,
          metaData.getVertexPropertyMetaData(vertexLabel)));
        outputStream.writeBytes(CSVConstants.ROW_DELIMITER);
      }

      for (String edgeLabel : metaData.getEdgeLabels()) {
        outputStream.writeBytes(constructMetaDataString(
          MetaDataSource.EDGE_TYPE,
          edgeLabel,
          metaData.getEdgePropertyMetaData(edgeLabel)));
        outputStream.writeBytes(CSVConstants.ROW_DELIMITER);
      }

      outputStream.flush();
      outputStream.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Construct a string representing the meta data of an entity, specified by its label.
   *
   * @param entityPrefix     entity prefix (g,v,e)
   * @param label            label of the entity
   * @param propertyMetaData list of property meta data
   * @return string representing all the meta data
   */
  private String constructMetaDataString(
    String entityPrefix,
    String label,
    List<PropertyMetaData> propertyMetaData) {
    StringBuilder metaDataBuilder = new StringBuilder();
    metaDataBuilder.append(entityPrefix);
    metaDataBuilder.append(CSVConstants.TOKEN_DELIMITER);
    metaDataBuilder.append(label);
    metaDataBuilder.append(CSVConstants.TOKEN_DELIMITER);
    for (int i = 0; i < propertyMetaData.size(); i++) {
      PropertyMetaData property = propertyMetaData.get(i);
      metaDataBuilder.append(property.getKey());
      metaDataBuilder.append(PropertyMetaData.PROPERTY_TOKEN_DELIMITER);
      metaDataBuilder.append(property.getTypeString());
      if (i < propertyMetaData.size() - 1) {
        metaDataBuilder.append(PropertyMetaData.PROPERTY_DELIMITER);
      }
    }
    return metaDataBuilder.toString();
  }
}
