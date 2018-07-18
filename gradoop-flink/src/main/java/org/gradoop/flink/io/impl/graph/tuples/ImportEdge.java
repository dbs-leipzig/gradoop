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
package org.gradoop.flink.io.impl.graph.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents an external edge which can be imported into the EPGM.
 *
 * f0: edge identifier or null
 * f1: source vertex identifier
 * f2: target vertex identifier
 * f3: edge label
 * f4: edge properties
 *
 * @param <K> vertex/edge identifier type
 */
public class ImportEdge<K extends Comparable<K>>
  extends Tuple5<K, K, K, String, Properties> {

  /**
   * Default constructor for (de-)serialization.
   */
  public ImportEdge() { }

  /**
   * Creates a new import edge.
   *
   * @param edgeId    import edge id (i.e. identifier in source system)
   * @param sourceId  import source vertex id
   * @param targetId  import target vertex id
   */
  public ImportEdge(K edgeId, K sourceId, K targetId) {
    this(edgeId, sourceId, targetId, GradoopConstants.DEFAULT_EDGE_LABEL);
  }

  /**
   * Creates a new import edge.
   *
   * @param edgeId    import edge id (i.e. identifier in source system)
   * @param sourceId  import source vertex id
   * @param targetId  import target vertex id
   * @param label     edge label
   */
  public ImportEdge(K edgeId, K sourceId, K targetId, String label) {
    this(edgeId, sourceId, targetId, label,
      Properties.createWithCapacity(0));
  }

  /**
   * Creates a new import edge.
   *
   * @param edgeId      import edge id (i.e. identifier in source system)
   * @param sourceId    import source vertex id
   * @param targetId    import target vertex id
   * @param label       edge label
   * @param properties  edge properties
   */
  public ImportEdge(K edgeId, K sourceId, K targetId, String label,
    Properties properties) {
    setId(edgeId);
    setSourceId(sourceId);
    setTargetId(targetId);
    setLabel(label);
    setProperties(properties);
  }

  public K getId() {
    return f0;
  }

  public void setId(K id) {
    this.f0 = checkNotNull(id, "id was null");
  }

  public K getSourceId() {
    return f1;
  }

  public void setSourceId(K sourceVertexId) {
    f1 = checkNotNull(sourceVertexId, "sourceId was null");
  }

  public K getTargetId() {
    return f2;
  }

  public void setTargetId(K targetVertexId) {
    f2 = checkNotNull(targetVertexId, "targetId was null");
  }

  public String getLabel() {
    return f3;
  }

  public void setLabel(String label) {
    f3 = checkNotNull(label, "label was null");
  }

  public Properties getProperties() {
    return f4;
  }

  public void setProperties(Properties properties) {
    f4 = checkNotNull(properties, "properties were null");
  }
}
