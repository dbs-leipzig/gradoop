
package org.gradoop.flink.io.impl.graph.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents an external vertex  which can be imported into the EPGM.
 *
 * f0: external vertex identifier
 * f1: vertex label
 * f2: vertex properties
 *
 * @param <K> vertex/edge identifier type
 */
public class ImportVertex<K extends Comparable<K>>
  extends Tuple3<K, String, Properties> {

  /**
   * Default constructor for (de-)serialization.
   */
  public ImportVertex() { }

  /**
   * Creates a new import vertex.
   *
   * @param id import vertex id (i.e. identifier in the source system)
   */
  public ImportVertex(K id) {
    this(id, GConstants.DEFAULT_VERTEX_LABEL);
  }

  /**
   * Creates a new import vertex.
   *
   * @param id    import vertex id (i.e. identifier in the source system)*
   * @param label vertex label
   */
  public ImportVertex(K id, String label) {
    this(id, label, Properties.createWithCapacity(0));
  }


  /**
   * Creates a new import vertex.
   *
   * @param id          import vertex id (i.e. identifier in the source system)
   * @param label       vertex label
   * @param properties  vertex properties
   */
  public ImportVertex(K id, String label, Properties properties) {
    setId(id);
    setLabel(label);
    setProperties(properties);
  }

  public K getId() {
    return f0;
  }

  public void setId(K id) {
    f0 = checkNotNull(id, "id was null");
  }

  public String getLabel() {
    return f1;
  }

  public void setLabel(String label) {
    f1 = checkNotNull(label, "label was null");
  }

  public Properties getProperties() {
    return f2;
  }

  public void setProperties(Properties properties) {
    f2 = checkNotNull(properties, "properties were null");
  }
}
