package org.gradoop.flink.io.impl.rdbms.constants;

/**
 * Stores constants for rdbms to graph conversation
 */
public class RDBMSConstants {
	
	/**
	 * Vertex key identifier for primary keys
	 */
	public static final String PK_ID = "*#primary_key_vertex_key_identifier#*";
	
	/**
	 * Field identifier for primary keys
	 */
	public static final String PK_FIELD = "pk";
	
	/**
	 * Field identifier for foreign keys
	 */
	public static final String FK_FIELD = "fk";
	
	/**
	 * Field identifier for further attributes
	 */
	public static final String ATTRIBUTE_FIELD = "att";
}
