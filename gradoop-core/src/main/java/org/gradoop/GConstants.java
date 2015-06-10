package org.gradoop;

import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.VertexHandler;

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
   * Default label for unlabeled vertices.
   */
  public static final String DEFAULT_VERTEX_LABEL = "__V_LABEL";
  /**
   * Default label for unlabeled graphs.
   */
  public static final String DEFAULT_GRAPH_LABEL = "__G_LABEL";
  /**
   * Default label for unlabeled edges.
   */
  public static final String DEFAULT_EDGE_LABEL = "__E_LABEL";

  /**
   * Column family name for label.
   */
  public static final String CF_META = "m";
  /**
   * Column name for label.
   */
  public static final String COL_LABEL = "t";
  /**
   * Column name for graphs.
   */
  public static final String COL_GRAPHS = "g";
  /**
   * Column family name for properties.
   */
  public static final String CF_PROPERTIES = "p";
  /**
   * Column family name for vertices.
   */
  public static final String CF_VERTICES = "v";
  /**
   * Column family name for outgoing edges.
   */
  public static final String CF_OUT_EDGES = "oe";
  /**
   * Column family name for incoming edges.
   */
  public static final String CF_IN_EDGES = "ie";
  /**
   * Column family name for graphs.
   */
  public static final String CF_GRAPHS = "g";

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

  /**
   * Configuration key to define a custom vertex handler.
   */
  public static final String VERTEX_HANDLER_CLASS = "gradoop.io.vertexhandler";

  /**
   * Default vertex handler which is used if no vertex handler is defined in
   * the job configuration.
   */
  public static final Class<? extends VertexHandler> DEFAULT_VERTEX_HANDLER =
    EPGVertexHandler.class;

  /**
   * Configuration key to define a custom graph handler.
   */
  public static final String GRAPH_HANDLER_CLASS = "gradoop.io.graphhandler";

  /**
   * Default graph handler which is used if no graph handler is defined the
   * job configuration.
   */
  public static final Class<? extends GraphHandler> DEFAULT_GRAPH_HANDLER =
    EPGGraphHandler.class;


}
