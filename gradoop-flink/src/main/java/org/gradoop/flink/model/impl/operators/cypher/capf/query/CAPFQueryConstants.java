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
package org.gradoop.flink.model.impl.operators.cypher.capf.query;

/**
 * Class containing string constants used for the execution of cypher queries via CAPF.
 */
class CAPFQueryConstants {

  /**
   * Constant used to name the id field in a node table
   */
  static final String NODE_ID = "node_id";

  /**
   * Constant used to name the id field in an edge table
   */
  static final String EDGE_ID = "edge_id";


  /**
   * Constant used to name the start node field in an edge table
   */
  static final String START_NODE = "start_node";


  /**
   * Constant used to name the end node field in an edge table
   */
  static final String END_NODE = "end_node";

  /**
   * Constant used to mark a field as containing a property value
   */
  static final String PROPERTY_PREFIX = "prop_";


  /**
   * Constant used as broadcast name for the count offset
   */
  static final String OFFSET = "offset";

}
