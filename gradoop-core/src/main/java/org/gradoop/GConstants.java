package org.gradoop;

/**
 * Constants used in Gradoop.
 */
public final class GConstants {
  /**
   * Default HBase table name for vertices.
   */
  public static final String DEFAULT_TABLE_VERTICES = "vertices";
  /**
   * Default HBase table name for graphs.
   */
  public static final String DEFAULT_TABLE_GRAPHS = "graphs";

  /**
   * Column family name for labels.
   */
  public static final String CF_LABELS = "labels";
  /**
   * Column family name for properties.
   */
  public static final String CF_PROPERTIES = "properties";
  /**
   * Column family name for vertices.
   */
  public static final String CF_VERTICES = "vertices";
  /**
   * Column family name for outgoing edges.
   */
  public static final String CF_OUT_EDGES = "out_edges";
  /**
   * Column family name for incoming edges.
   */
  public static final String CF_IN_EDGES = "in_edges";
  /**
   * Column family name for graphs.
   */
  public static final String CF_GRAPHS = "graphs";

  /**
   * Default label for unlabeled edges.
   */
  public static final String DEFAULT_EDGE_LABEL = "E_LABEL";

  /**
   * {@code <property-type>} for {@link java.lang.Boolean}
   */
  public static final byte TYPE_BOOLEAN = 0x00;
  /**
   * {@code <property-type>} for {@link java.lang.Integer}
   */
  public static final byte TYPE_INTEGER = 0x01;
  /**
   * {@code <property-type>} for {@link java.lang.Long}
   */
  public static final byte TYPE_LONG = 0x02;
  /**
   * {@code <property-type>} for {@link java.lang.Float}
   */
  public static final byte TYPE_FLOAT = 0x03;
  /**
   * {@code <property-type>} for {@link java.lang.Double}
   */
  public static final byte TYPE_DOUBLE = 0x04;
  /**
   * {@code <property-type>} for {@link java.lang.String}
   */
  public static final byte TYPE_STRING = 0x05;
}
