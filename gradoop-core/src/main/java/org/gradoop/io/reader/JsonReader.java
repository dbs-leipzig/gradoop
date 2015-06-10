package org.gradoop.io.reader;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.io.writer.JsonWriter;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.impl.EdgeFactory;
import org.gradoop.model.impl.VertexFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Creates a vertex from its Json representation.
 */
public class JsonReader extends SingleVertexReader {

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex readVertex(String line) {
    Vertex v = null;
    try {
      JSONObject json = new JSONObject(line);
      // vertex id
      Long vertexID = readVertexID(json);
      // vertex label
      String label = null;
      if (json.has(JsonWriter.LABEL)) {
        label = readLabel(json);
      }
      // vertex properties
      Map<String, Object> properties = null;
      if (json.has(JsonWriter.PROPERTIES)) {
        properties = readProperties(json.getJSONObject(JsonWriter.PROPERTIES));
      }
      // outgoing edges
      Iterable<Edge> outgoingEdges = null;
      if (json.has(JsonWriter.OUT_EDGES)) {
        outgoingEdges = readEdges(json.getJSONArray(JsonWriter.OUT_EDGES));
      }
      // incoming edges
      Iterable<Edge> incomingEdges = null;
      if (json.has(JsonWriter.IN_EDGES)) {
        incomingEdges = readEdges(json.getJSONArray(JsonWriter.IN_EDGES));
      }
      // graphs
      Iterable<Long> graphs = null;
      if (json.has(JsonWriter.GRAPHS)) {
        graphs = readGraphs(json.getJSONArray(JsonWriter.GRAPHS));
      }
      v = VertexFactory
        .createDefaultVertex(vertexID, label, properties, outgoingEdges,
          incomingEdges, graphs);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return v;
  }

  /**
   * Reads the vertex id from the json object.
   *
   * @param json json object
   * @return vertex id
   * @throws JSONException
   */
  private Long readVertexID(final JSONObject json) throws JSONException {
    return json.getLong(JsonWriter.VERTEX_ID);
  }

  /**
   * Reads the vertex label from the json object.
   *
   * @param json json object
   * @return vertex label
   * @throws JSONException
   */
  private String readLabel(final JSONObject json) throws
    JSONException {
    return json.getString(JsonWriter.LABEL);
  }

  /**
   * Reads key value pairs from the given json object.
   *
   * @param propertiesObject property key value pairs
   * @return properties
   * @throws JSONException
   */
  private Map<String, Object> readProperties(
    final JSONObject propertiesObject) throws JSONException {
    Map<String, Object> properties =
      Maps.newHashMapWithExpectedSize(propertiesObject.length());
    Iterator<?> keys = propertiesObject.keys();
    while (keys.hasNext()) {
      String key = keys.next().toString();
      Object o = propertiesObject.get(key);
      properties.put(key, o);
    }
    return properties;
  }

  /**
   * Reads edges from json array.
   *
   * @param edgeArray json array with edge information.
   * @return edges
   * @throws JSONException
   */
  private Iterable<Edge> readEdges(final JSONArray edgeArray) throws
    JSONException {
    List<Edge> edges = Lists.newArrayListWithCapacity(edgeArray.length());
    for (int i = 0; i < edgeArray.length(); i++) {
      JSONObject edge = edgeArray.getJSONObject(i);
      String label = edge.getString(JsonWriter.EDGE_LABEL);
      Long otherID = edge.getLong(JsonWriter.EDGE_OTHER_ID);
      if (edge.has(JsonWriter.PROPERTIES)) {
        edges.add(EdgeFactory.createDefaultEdge(otherID, label, (long) i,
          readProperties(edge.getJSONObject(JsonWriter.PROPERTIES))));
      } else {
        edges.add(
          EdgeFactory.createDefaultEdgeWithLabel(otherID, label, (long) i));
      }
    }
    return edges;
  }

  /**
   * Reads graphs from a given json array.
   *
   * @param graphArray json array with graphs
   * @return graphs
   * @throws JSONException
   */
  private Iterable<Long> readGraphs(final JSONArray graphArray) throws
    JSONException {
    List<Long> graphs = Lists.newArrayListWithCapacity(graphArray.length());
    for (int i = 0; i < graphArray.length(); i++) {
      graphs.add(graphArray.getLong(i));
    }
    return graphs;
  }
}
