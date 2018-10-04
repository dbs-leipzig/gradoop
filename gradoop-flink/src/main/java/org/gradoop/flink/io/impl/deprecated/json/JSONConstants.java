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
package org.gradoop.flink.io.impl.deprecated.json;

/**
 * Constants needed for the JSON IO classes.
 */
public class JSONConstants {

  /**
   * Key for vertex, edge and graph id.
   */
  public static final String IDENTIFIER = "id";
  /**
   * Key for meta Json object.
   */
  public static final String META = "meta";
  /**
   * Key for data Json object.
   */
  public static final String DATA = "data";
  /**
   * Key for vertex, edge and graph label.
   */
  public static final String LABEL = "label";
  /**
   * Key for graph identifiers at vertices and edges.
   */
  public static final String GRAPHS = "graphs";
  /**
   * Key for vertex identifiers at graphs.
   */
  public static final String VERTICES = "vertices";
  /**
   * Key for edge identifiers at graphs.
   */
  public static final String EDGES = "edges";
  /**
   * Key for edge source vertex id.
   */
  public static final String EDGE_SOURCE = "source";
  /**
   * Key for edge target vertex id.
   */
  public static final String EDGE_TARGET = "target";
}
