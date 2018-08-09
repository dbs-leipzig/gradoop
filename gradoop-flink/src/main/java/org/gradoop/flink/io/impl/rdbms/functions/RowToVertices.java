package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;

/**
 * Creates vertices from database input.
 */
public class RowToVertices extends RichMapFunction<Row, Vertex> {
	
	/**
	 * List of all instances converted to vertices
	 */
	private List<TableToNode> tables;
	
	/**
	 * Current table
	 */
	private TableToNode currentTable;
	
	/**
	 * Current rowheader
	 */
	private RowHeader rowheader;
	
	/**
	 * Name of current database table
	 */
	private String tableName;
	
	/**
	 * Current position of iteration
	 */
	private int tablePos;
	
	/**
	 * Constructor
	 * 
	 * @param tableName Name of current database table
	 */
	public RowToVertices(String tableName, int tablePos){
		this.tableName = tableName;
		this.tablePos = tablePos;
	}
	
	@Override
	public Vertex map(Row tuple) throws Exception {
		this.currentTable = tables.get(tablePos);
		this.rowheader = currentTable.getRowheader();
		
		Vertex v = new Vertex();
		v.setId(GradoopId.get());
		v.setLabel(tableName);
		Properties props = AttributesToProperties.getProperties(tuple, rowheader);
		props.set(RdbmsConstants.PK_ID,PrimaryKeyConcatString.getPrimaryKeyString(tuple,rowheader));
		v.setProperties(props);
		
		return v;
	}

	public void open(Configuration parameters) throws Exception {
		this.tables = getRuntimeContext().getBroadcastVariable("tables");
	}
}
