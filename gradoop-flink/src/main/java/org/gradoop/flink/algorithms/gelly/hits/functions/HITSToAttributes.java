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
package org.gradoop.flink.algorithms.gelly.hits.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.library.linkanalysis.HITS;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Stores HITS Results as Properties of a EPGMVertex
 */
public class HITSToAttributes implements JoinFunction<HITS.Result<GradoopId>, EPGMVertex, EPGMVertex> {

  /**
   * Property Key to store the authority score
   */
  private String authorityPropertyKey;
  /**
   * Property key to store the hub score.
   */
  private String hubPropertyKey;

  /**
   * Creates a {@link HITSToAttributes} instance.
   *
   * @param authorityPropertyKey Property key to store the authority score of a vertex
   * @param hubPropertyKey       Property key to store the hub score of a vertex
   */
  public HITSToAttributes(String authorityPropertyKey, String hubPropertyKey) {
    this.authorityPropertyKey = authorityPropertyKey;
    this.hubPropertyKey = hubPropertyKey;
  }

  @Override
  public EPGMVertex join(HITS.Result<GradoopId> result, EPGMVertex vertex) throws Exception {
    vertex.setProperty(authorityPropertyKey,
      PropertyValue.create(result.getAuthorityScore().getValue()));
    vertex.setProperty(hubPropertyKey, PropertyValue.create(result.getHubScore().getValue()));
    return vertex;
  }
}
