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
package org.gradoop.flink.algorithms.gelly.hits.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.library.linkanalysis.HITS;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Stores HITS Results as Properties of a Vertex
 */
public class HITSToAttributes implements JoinFunction<HITS.Result<GradoopId>, Vertex, Vertex> {

  /**
   * Property Key to store the authority score
   */
  private String authorityPropertyKey;
  /**
   * Property key to store the hub score.
   */
  private String hubPropertyKey;

  /**
   * @param authorityPropertyKey Property key to store the authority score of a vertex
   * @param hubPropertyKey       Property key to store the hub score of a vertex
   */
  public HITSToAttributes(String authorityPropertyKey, String hubPropertyKey) {
    this.authorityPropertyKey = authorityPropertyKey;
    this.hubPropertyKey = hubPropertyKey;
  }

  @Override
  public Vertex join(HITS.Result<GradoopId> result, Vertex vertex) throws Exception {
    vertex.setProperty(authorityPropertyKey,
      PropertyValue.create(result.getAuthorityScore().getValue()));
    vertex.setProperty(hubPropertyKey, PropertyValue.create(result.getHubScore().getValue()));
    return vertex;
  }
}
