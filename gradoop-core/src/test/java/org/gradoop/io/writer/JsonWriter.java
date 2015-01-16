package org.gradoop.io.writer;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;

/**
 * Converts a vertex into a json representation.
 */
public class JsonWriter implements VertexLineWriter {

  public static final String VERTEX_ID = "id";

  public static final String LABELS = "labels";

  public static final String PROPERTIES = "properties";

  public static final String OUT_EDGES = "out-edges";

  public static final String IN_EDGES = "in-edges";

  public static final String GRAPHS = "graphs";

  public static final String EDGE_OTHER_ID = "otherid";

  public static final String EDGE_LABEL = "label";

  @Override
  public String writeVertex(Vertex vertex) {
    JSONObject json = new JSONObject();
    try {
      json.put(VERTEX_ID, vertex.getID());
      json = writeLabels(json, vertex);
      json = writeProperties(json, vertex);
      json = writeEdges(json, OUT_EDGES, vertex.getOutgoingEdges());
      json = writeEdges(json, IN_EDGES, vertex.getIncomingEdges());
      json = writeGraphs(json, vertex);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return json.toString();
  }

  private JSONObject writeLabels(final JSONObject json, final Vertex v) throws
    JSONException {
    JSONArray labelArray = new JSONArray();
    for (String label : v.getLabels()) {
      labelArray.put(label);
    }
    json.put(LABELS, labelArray);
    return json;
  }

  private JSONObject writeProperties(final JSONObject json,
    final Vertex v) throws JSONException {
    JSONObject properties = new JSONObject();
    for (String propertyKey : v.getPropertyKeys()) {
      properties.put(propertyKey, v.getProperty(propertyKey));
    }
    json.put(PROPERTIES, properties);
    return json;
  }

  private JSONObject writeEdges(final JSONObject json, final String key,
    final Iterable<Edge> edges) throws JSONException {
    JSONArray edgeArray = new JSONArray();
    for (Edge e : edges) {
      JSONObject jsonEdge = new JSONObject();
      jsonEdge.put(EDGE_OTHER_ID, e.getOtherID());
      jsonEdge.put(EDGE_LABEL, e.getLabel());
      edgeArray.put(jsonEdge);
    }
    json.put(key, edgeArray);
    return json;
  }

  private JSONObject writeGraphs(final JSONObject json, final Vertex v) throws
    JSONException {
    JSONArray graphArray = new JSONArray();
    for (Long graph : v.getGraphs()) {
      graphArray.put(graph);
    }
    json.put(GRAPHS, graphArray);
    return json;
  }
}
