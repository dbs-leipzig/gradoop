package org.gradoop.flink.algorithms.gelly.hits;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.library.link_analysis.HITS;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Stores HITS Results as Properties of a Vertex
 */
public class HITSToAttributes implements JoinFunction<HITS.Result<GradoopId>, Vertex, Vertex> {

  private String authorityPropertyKey;
  private String hubPropertyKey;

  /**
   * @param authorityPropertyKey Property key to store the authority score.
   * @param hubPropertyKey       Property key to store the hub score.
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
