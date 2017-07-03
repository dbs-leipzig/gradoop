
package org.gradoop.flink.io.impl.json.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Reads graph data from a json document. The document contains at least
 * the graph id, an embedded data document and an embedded meta document.
 * The data document contains all key-value pairs stored at the graphs, the
 * meta document contains the graph label and the vertex/edge identifiers
 * of vertices/edges contained in that graph.
 * <p>
 * Example:
 * <p>
 * {
 * "id":0,
 * "data":{"title":"Graph Databases"},
 * "meta":{"label":"Community","vertices":[0,1,2],"edges":[4,5,6]}
 * }
 */
public class JSONToGraphHead extends JSONToEntity
  implements MapFunction<String, GraphHead> {

  /**
   * Creates graph data objects
   */
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;

  /**
   * Creates map function
   *
   * @param epgmGraphHeadFactory graph data factory
   */
  public JSONToGraphHead(EPGMGraphHeadFactory<GraphHead> epgmGraphHeadFactory) {
    this.graphHeadFactory = epgmGraphHeadFactory;
  }

  /**
   * Creates graph data from JSON string representation.
   *
   * @param s json string representation
   * @return SubgraphWithCount storing graph data
   * @throws Exception
   */
  @Override
  public GraphHead map(String s) throws Exception {
    JSONObject jsonGraph = new JSONObject(s);
    GradoopId graphID = getID(jsonGraph);
    String label = getLabel(jsonGraph);
    Properties properties = Properties.createFromMap(
      getProperties(jsonGraph));

    return graphHeadFactory.initGraphHead(graphID, label, properties);
  }
}
