package org.gradoop.biiig.io.reader;

import com.google.common.collect.Lists;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.model.Attributed;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.inmemory.MemoryEdge;
import org.gradoop.model.inmemory.MemoryVertex;

import java.util.Iterator;
import java.util.Random;

/**
 * Reads Json input from Foodbroker, a data generator for business process data.
 */
public class FoodBrokerReader implements VertexLineReader {
  /**
   * Key for vertex id.
   */
  private static final String VERTEX_ID = "id";
  /**
   * Key for edge source vertex id.
   */
  private static final String EDGE_SOURCE = "start";
  /**
   * Key for edge target vertex id.
   */
  private static final String EDGE_TARGET = "end";

  /**
   * Key for edge type property.
   */
  private static final String TYPE_PROPERTY = "type";
  /**
   * Key for vertex kind property (MasterData, TransData).
   */
  private static final String KIND_PROPERTY = "kind";

  /**
   * Key for meta Json object.
   */
  private static final String META = "meta";
  /**
   * Key for data Json object.
   */
  private static final String DATA = "data";

  /**
   * Prefix gets attached to the meta property keys.
   */
  private static final String META_PREFIX = "__";
  /**
   * Just for internal use.
   */
  private static final String EMPTY_PREFIX = "";
  /**
   * Needed for random edge index generation.
   */
  private static final Random RANDOM = new Random();

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex readLine(String line) {
    Vertex v = null;
    try {
      JSONObject jsonObject = new JSONObject(line);
      if (jsonObject.has(EDGE_SOURCE)) {
        v = createFromEdge(jsonObject);
      } else {
        v = createFromVertex(jsonObject);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return v;
  }

  /**
   * Creates a vertex from an edge input line.
   *
   * @param edge json encoded edge object
   * @return Vertex with id and single outgoing edge
   * @throws JSONException
   */
  private Vertex createFromEdge(JSONObject edge)
    throws JSONException {
    Long sourceID = edge.getLong(EDGE_SOURCE);
    Long targetID = edge.getLong(EDGE_TARGET);

    Edge e = new MemoryEdge(targetID, getType(edge), RANDOM.nextLong());
    addProperties(e, edge.getJSONObject(META), META_PREFIX);
    addProperties(e, edge.getJSONObject(DATA));

    return new MemoryVertex(sourceID, null, null, Lists.newArrayList(e),
      null, null);
  }

  /**
   * Creates a vertex from a vertex input line.
   *
   * @param vertex json encoded vertex object
   * @return vertex with id, labels, properties
   * @throws JSONException
   */
  private Vertex createFromVertex(JSONObject vertex)
    throws JSONException {
    Long vertexID = vertex.getLong(VERTEX_ID);
    Vertex v = new MemoryVertex(vertexID);
    addProperties(v, vertex.getJSONObject(META), META_PREFIX);
    addProperties(v, vertex.getJSONObject(DATA));
    v.addLabel(getKind(vertex));
    return v;
  }

  /**
   * Returns the type property which is needed for edge labels.
   *
   * @param object json encoded edge
   * @return edge type (label)
   * @throws JSONException
   */
  private String getType(JSONObject object)
    throws JSONException {
    return object.getJSONObject(META).getString(TYPE_PROPERTY);
  }

  /**
   * Returns the kind property which is needed for vertex labels. Can be
   * MasterData or TransData.
   *
   * @param object json encoded vertex
   * @return vertex type
   * @throws JSONException
   */
  private String getKind(JSONObject object)
    throws JSONException {
    return object.getJSONObject(META).getString(KIND_PROPERTY);
  }

  /**
   * Adds the META and DATA entries to the entity properties.
   *
   * @param attributed an entity with attributes
   * @param object     json encoded vertex or edge
   * @throws JSONException
   */
  private void addProperties(final Attributed attributed,
                             final JSONObject object)
    throws JSONException {
    addProperties(attributed, object, "");
  }

  /**
   * Adds the META and DATA entries to the entity properties.
   *
   * @param attributed an entity with attributes
   * @param object     json encoded vertex or edge
   * @param prefix     prefix for meta property keys
   * @throws JSONException
   */
  private void addProperties(final Attributed attributed,
                             final JSONObject object,
                             final String prefix)
    throws JSONException {
    boolean addPrefix = !EMPTY_PREFIX.equals(prefix);
    Iterator<?> keys = object.keys();
    while (keys.hasNext()) {
      String key = keys.next().toString();
      Object o = object.get(key);
      key = (addPrefix) ? prefix + key : key;
      attributed.addProperty(key, o);
    }
  }
}
