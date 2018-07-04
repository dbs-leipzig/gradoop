package org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.functions.AttributesToProperties;
import org.gradoop.flink.io.impl.rdbms.metadata.RDBMSTableBase;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;
import org.gradoop.flink.io.impl.rdbms.tuples.PKTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

public class TupleToVertex extends RichMapFunction<Row,ImportVertex<String>>{
	private List<TablesToNodes> tables;
	private int tablePos;
	private TablesToNodes currentTable;
	
	public TupleToVertex(int tablePos){
		this.tablePos = tablePos;
	}
	@Override
	public ImportVertex map(Row tuple) throws Exception {
		this.currentTable = tables.get(tablePos);
		ImportVertex<String> v = new ImportVertex<String>();
		String concatString = concatPK(tuple);
		v.setId(concatString);
		v.setLabel(tables.get(tablePos).getTableName());
		v.setProperties(AttributesToProperties.getProperties(tuple, currentTable.getRowheader()));
		
		return v;
	}
	
	public String concatPK(Row tuple) {
		String pkConcat = "";
		if (currentTable.getPrimaryKeys().size() > 1) {
			for (PKTuple pk : currentTable.getPrimaryKeys()) {
				pkConcat += tuple.getField(pk.getPos()) + RDBMSConstants.PK_DELIMITER;
			}
		} else {
			pkConcat = tuple.getField(currentTable.getPrimaryKeys().get(0).getPos()).toString();
		}
		return pkConcat;
	}
	
	public void open(Configuration parameters) throws Exception {
		this.tables = getRuntimeContext().getBroadcastVariable("tables");
	}
}
