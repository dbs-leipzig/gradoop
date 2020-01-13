/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.api.layouts;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.ElementFactoryProvider;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base interface for layout factories.
 *
 * @param <G> GraphHead type.
 * @param <V> Vertex type.
 * @param <E> Edge type.
 */
public interface BaseLayoutFactory<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge> extends ElementFactoryProvider<G, V, E> {
  /**
   * Sets the config.
   *
   * @param config Gradoop flink config
   */
  void setGradoopFlinkConfig(GradoopFlinkConfig config);
}
