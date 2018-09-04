package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;

/**
 * Creates one EPGM vertex from one row 
 */
public class RowToVertices extends RichMapFunction<Row, Vertex> {
	
	/**
	 * EPGM vertex factory
	 */
	private EPGMVertexFactory vertexFactory;
	
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
	public RowToVertices(EPGMVertexFactory vertexFactory, String tableName, int tablePos){
		this.vertexFactory = vertexFactory;
		this.tableName = tableName;
		this.tablePos = tablePos;
	}
	
	@Override
	public Vertex map(Row tuple) throws Exception {
		this.currentTable = tables.get(tablePos);
		this.rowheader = currentTable.getRowheader();
		
		
		GradoopId id = GradoopId.get();
		String label = tableName;
		Properties properties = AttributesToProperties.getProperties(tuple, rowheader);
		properties.set(RdbmsConstants.PK_ID,PrimaryKeyConcatString.getPrimaryKeyString(tuple,rowheader));
		
		return (Vertex) vertexFactory.initVertex(id, label, properties);
	}

	public void open(Configuration parameters) throws Exception {
		this.tables = getRuntimeContext().getBroadcastVariable("tables");
	}
}
