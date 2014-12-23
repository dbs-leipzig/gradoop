package org.gradoop.biiig.io.reader;

import com.google.common.collect.Lists;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.biiig.BIIIGConstants;
import org.gradoop.biiig.io.formats.BTGVertexType;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.model.Attributed;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.inmemory.EdgeFactory;
import org.gradoop.model.inmemory.MemoryVertex;

import java.util.Iterator;
import java.util.List;
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
  public Vertex readVertex(String line) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Vertex> readVertexList(String line) {
    List<Vertex> vList = null;
    try {
      JSONObject jsonObject = new JSONObject(line);
      if (jsonObject.has(EDGE_SOURCE)) {
        vList = createFromEdgeLine(jsonObject);
      } else {
        vList = createFromVertexLine(jsonObject);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return vList;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsVertexLists() {
    return true;
  }

  /**
   * Creates a vertex from an edge input line.
   *
   * @param edge json encoded edge object
   * @return Vertex with id and single outgoing edge
   * @throws JSONException
   */
  private List<Vertex> createFromEdgeLine(JSONObject edge)
    throws JSONException {
    List<Vertex> vList = Lists.newArrayListWithCapacity(2);
    Long sourceID = edge.getLong(EDGE_SOURCE);
    Long targetID = edge.getLong(EDGE_TARGET);

    String edgeType = getType(edge);

    // outgoing edge on source vertex
    Edge edgeOut = EdgeFactory.createDefaultEdge(targetID, edgeType, RANDOM
      .nextLong());
    addProperties(edgeOut, edge.getJSONObject(META),
      BIIIGConstants.META_PREFIX);
    addProperties(edgeOut, edge.getJSONObject(DATA));
    vList.add(new MemoryVertex(sourceID, null, null,
      Lists.newArrayList(edgeOut), null, null));

    // incoming edge on target vertex
    Edge edgeIn = EdgeFactory.createDefaultEdge(sourceID, edgeType, RANDOM
      .nextLong());
    addProperties(edgeIn, edge.getJSONObject(META), BIIIGConstants.META_PREFIX);
    addProperties(edgeIn, edge.getJSONObject(DATA));
    vList.add(new MemoryVertex(targetID, null, null, null,
      Lists.newArrayList(edgeIn), null));

    return vList;
  }

  /**
   * Creates a vertex from a vertex input line.
   *
   * @param vertex json encoded vertex object
   * @return vertex with id, labels, properties
   * @throws JSONException
   */
  private List<Vertex> createFromVertexLine(JSONObject vertex)
    throws JSONException {
    Long vertexID = vertex.getLong(VERTEX_ID);
    Vertex v = new MemoryVertex(vertexID);
    addProperties(v, vertex.getJSONObject(META), BIIIGConstants.META_PREFIX);
    addProperties(v, vertex.getJSONObject(DATA));
    v.addLabel(String.valueOf(getKind(vertex)));
    return Lists.newArrayList(v);
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
  private int getKind(JSONObject object)
    throws JSONException {
    String kindValue = object.getJSONObject(META).getString(KIND_PROPERTY);
    return (kindValue.equals(BTGVertexType.MASTER.toString())) ?
      BTGVertexType.MASTER.ordinal() : BTGVertexType.TRANSACTIONAL.ordinal();
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
