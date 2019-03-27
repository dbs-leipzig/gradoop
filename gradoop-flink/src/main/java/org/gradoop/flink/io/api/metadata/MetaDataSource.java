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
package org.gradoop.flink.io.api.metadata;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.common.model.impl.metadata.PropertyMetaData;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.io.api.metadata.functions.ElementToPropertyMetaData;
import org.gradoop.flink.io.api.metadata.functions.ReducePropertyMetaData;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base interface for factories that create metadata objects from tuples and files (distributed
 * or locally).
 *
 * @param <M> meta data type
 */
public interface MetaDataSource<M extends MetaData> {
  /**
   * Used to tag a graph head entity.
   */
  String GRAPH_TYPE = "g";
  /**
   * Used to tag a vertex entity.
   */
  String VERTEX_TYPE = "v";
  /**
   * Used to tag an edge entity.
   */
  String EDGE_TYPE = "e";

  /**
   * Creates the meta data for the given graph.
   *
   * @param graph logical graph
   * @return meta data information
   */
  default DataSet<Tuple3<String, String, String>> tuplesFromGraph(LogicalGraph graph) {
    return tuplesFromElements(graph.getVertices())
      .union(tuplesFromElements(graph.getEdges()));
  }

  /**
   * Creates the meta data for the given graph collection.
   *
   * @param graphs graph collection
   * @return meta data information
   */
  default DataSet<Tuple3<String, String, String>> tuplesFromCollection(GraphCollection graphs) {
    return tuplesFromElements(graphs.getVertices())
      .union(tuplesFromElements(graphs.getEdges()))
      .union(tuplesFromElements(graphs.getGraphHeads()));
  }

  /**
   * Creates the meta data for the specified data set of EPGM elements.
   *
   * @param elements EPGM elements
   * @param <E>      EPGM element type
   * @return meta data information
   */
  static <E extends Element> DataSet<Tuple3<String, String, String>> tuplesFromElements(
    DataSet<E> elements) {
    return elements
      .map(new ElementToPropertyMetaData<>())
      .groupBy(0, 1)
      .reduce(new ReducePropertyMetaData())
      .map(tuple -> Tuple3.of(
        tuple.f0,
        tuple.f1,
        tuple.f2.stream().sorted().collect(
          Collectors.joining(PropertyMetaData.PROPERTY_DELIMITER))))
      .returns(new TupleTypeInfo<>(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO))
      .withForwardedFields("f0", "f1");
  }

  /**
   * Creates a {@link MetaData} object from the specified lines. The specified tuple is already
   * separated into the label and the metadata.
   *
   * @param metaDataStrings (element prefix (g,v,e), label, meta data) tuples
   * @return Meta Data object
   */
  M fromTuples(List<Tuple3<String, String, String>> metaDataStrings);

  /**
   * Reads the meta data from a specified file. Each line is split into a
   * (element prefix, label, metadata) tuple
   * and put into a dataset. The latter can be used to broadcast the metadata to the mappers.
   *
   * @param path   path to metadata csv file
   * @param config gradoop configuration
   * @return (element prefix ( g, v, e), label, metadata) tuple dataset
   */
  DataSet<Tuple3<String, String, String>> readDistributed(String path, GradoopFlinkConfig config);

  /**
   * Reads the meta data from a specified csv file. The file can be either located in a local file
   * system or in HDFS.
   *
   * @param path       path to metadata csv file
   * @param hdfsConfig file system configuration
   * @return meta data
   */
  M readLocal(String path, Configuration hdfsConfig) throws IOException;
}
