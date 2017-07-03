
package org.gradoop.common.config;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;

/**
 * Basic Gradoop Configuration.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GradoopConfig
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  private final GraphHeadFactory graphHeadFactory;
  
  private final VertexFactory vertexFactory;

  private final EdgeFactory edgeFactory;

  /**
   * Creates a new Configuration.
   *
   * @param graphHeadHandler  graph head handler
   * @param vertexHandler     vertex handler
   * @param edgeHandler       edge handler
   */
  @SuppressWarnings("unchecked")
  protected GradoopConfig() {
	  this.graphHeadFactory = new GraphHeadFactory();
	  this.vertexFactory = new VertexFactory();
	  this.edgeFactory = new EdgeFactory();
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads.
   *
   * @return Default Gradoop configuration.
   */
  public static GradoopConfig<GraphHead, Vertex, Edge> getDefaultConfig() {
    return new GradoopConfig<>();
  }

  public GraphHeadFactory getGraphHeadFactory() {
    return graphHeadFactory;
  }

  public VertexFactory getVertexFactory() {
    return vertexFactory;
  }

  public EdgeFactory getEdgeFactory() {
    return edgeFactory;
  }
}
