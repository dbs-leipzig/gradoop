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
package org.gradoop.storage.impl.accumulo.constants;

/**
 * Accumulo table constants definition
 */
public class AccumuloTables {

  /**
   * gradoop edge table name
   */
  public static final String EDGE = "edge";

  /**
   * gradoop vertex table name
   */
  public static final String VERTEX = "vertex";

  /**
   * gradoop graph head table name
   */
  public static final String GRAPH = "graph";

  /**
   * gradoop predicate options key
   */
  public static final String KEY_PREDICATE = "__filter__";

  /**
   * cf or cq constants key
   */
  public static class KEY {

    /**
     * empty key
     */
    public static final String NONE = "";

    /**
     * element label
     */
    public static final String LABEL = "label";

    /**
     * edge source
     */
    public static final String SOURCE = "source";

    /**
     * edge target
     */
    public static final String TARGET = "target";

    /**
     * element property
     */
    public static final String PROPERTY = "property";

    /**
     * graph element belonging
     */
    public static final String GRAPH = "graph";

    /**
     * graph vertex edge in
     */
    public static final String EDGE_IN = "edge_in";

    /**
     * graph vertex edge out
     */
    public static final String EDGE_OUT = "edge_out";
  }

}
