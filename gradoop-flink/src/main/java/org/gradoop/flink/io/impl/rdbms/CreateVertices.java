package org.gradoop.flink.io.impl.rdbms;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.rdbms.functions.AttributesToProperties;
import org.gradoop.flink.io.impl.rdbms.functions.PrimaryKeyConcatString;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeader;

/**
 * class to convert rows to importvertices
 * @author pc
 *
 */
public class CreateVertices extends RichMapFunction<Row, ImportVertex<String>> {
	private List<RDBMSTable> table;
	private int tablePosition;
	
	public CreateVertices(int tablePosition){
		this.tablePosition = tablePosition;
	}
	
	@Override
	public ImportVertex<String> map(Row tuple) throws Exception {
		// TODO Auto-generated method stub
		RowHeader rowHeader = table.get(tablePosition).getRowHeader();
		String pkString = new PrimaryKeyConcatString().getPrimaryKeyString(tuple,rowHeader);
		ImportVertex<String> v = new ImportVertex<String>();
		
		v.setId(pkString);
		v.setLabel(table.get(tablePosition).getTableName());
		v.setProperties(AttributesToProperties.getProperties(tuple, rowHeader));
		return v;
	}

	public void open(Configuration parameters) throws Exception {
		this.table = getRuntimeContext().getBroadcastVariable("tables");
	}
}
