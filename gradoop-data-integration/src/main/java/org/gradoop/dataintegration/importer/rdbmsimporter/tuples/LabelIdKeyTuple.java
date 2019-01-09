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
package org.gradoop.dataintegration.importer.rdbmsimporter.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Represents primary key respectively foreign key site of foreign
 * key relation.
 * f0 : label
 * f1 : gradoop id
 * f2 : value string
 *
 */
public class LabelIdKeyTuple extends Tuple3<String, GradoopId, String> {

  /**
   * Default serial version uid.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Empty Constructor.
   */
  public LabelIdKeyTuple() { }

  /**
   * Creates an instance of {@link LabelIdKeyTuple} to store label, gradoop id and a key string.
   *
   * @param label Vertex label
   * @param gradoopId Valid gradoop id
   * @param key Key value string
   */
  public LabelIdKeyTuple(String label, GradoopId gradoopId, String key) {
    super(label, gradoopId, key);
  }

  /**
   * Get serial version uid.
   *
   * @return serial version uid
   */
  public static long getSerialVersionUID() {
    return serialVersionUID;
  }

  /**
   * Get label.
   *
   * @return label
   */
  public String getLabel() {
    return this.f0;
  }

  /**
   * Get gradoop id.
   *
   * @return gradoop id
   */
  public GradoopId getId() {
    return this.f1;
  }

  /**
   * Get key value.
   *
   * @return key value
   */
  public String getKey() {
    return this.f2;
  }

  /**
   * Set label.
   * @param label label
   */
  public void setLabel(String label) {
    this.f0 = label;
  }

  /**
   * Set gradopp id.
   *
   * @param gradoopId gradoop id
   */
  public void setId(GradoopId gradoopId) {
    this.f1 = gradoopId;
  }

  /**
   * Set key value string.
   *
   * @param key key value string
   */
  public void setKey(String key) {
    this.f2 = key;
  }
}
