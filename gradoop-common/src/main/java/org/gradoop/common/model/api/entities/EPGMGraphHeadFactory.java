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
package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Initializes {@link EPGMGraphHead} objects of a given type.
 *
 * @param <G> EPGM graph head type
 */
public interface EPGMGraphHeadFactory<G extends EPGMGraphHead>
  extends EPGMElementFactory<G> {

  /**
   * Creates a new graph head based.
   *
   * @return graph data
   */
  G createGraphHead();

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id graph identifier
   * @return graph data
   */
  G initGraphHead(GradoopId id);

  /**
   * Creates a new graph head based on the given parameters.
   *
   * @param label graph label
   * @return graph data
   */
  G createGraphHead(String label);

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id    graph identifier
   * @param label graph label
   * @return graph data
   */
  G initGraphHead(GradoopId id, String label);

  /**
   * Creates a new graph head based on the given parameters.
   *
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  G createGraphHead(String label, Properties properties);

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  G initGraphHead(GradoopId id, String label, Properties properties);
}
