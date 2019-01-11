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
package org.gradoop.dataintegration.importer.impl.json;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.importer.impl.json.functions.MinimalJsonToVertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Objects;

/**
 * A data importer that creates a graph from JSON objects.
 * The input is expected to be a text file where every line is a valid JSON object.
 * A vertex will be created for each of those objects, properties will be created from
 * the attributes of the JSON object.
 */
public class MinimalJSONImporter implements DataSource {

  /**
   * The path of the JSON file or directory.
   */
  private final String jsonPath;

  /**
   * The config used to access vertex factories and for reading the JSON file.
   */
  private final GradoopFlinkConfig config;

  /**
   * Create a new simple JSON importer.
   *
   * @param jsonPath The path of the JSON file(s).
   * @param config   The config used to read the file(s) and create the graph.
   */
  public MinimalJSONImporter(String jsonPath, GradoopFlinkConfig config) {
    Objects.requireNonNull(jsonPath);
    Objects.requireNonNull(config);
    this.jsonPath = jsonPath;
    this.config = config;
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    DataSet<Vertex> vertices = config.getExecutionEnvironment().readTextFile(jsonPath)
      .map(new MinimalJsonToVertex(config.getVertexFactory()));
    return config.getLogicalGraphFactory().fromDataSets(vertices);
  }

  @Override
  public GraphCollection getGraphCollection() {
    return config.getGraphCollectionFactory().fromGraph(getLogicalGraph());
  }
}
