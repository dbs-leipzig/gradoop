package org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.functions.AttributesToProperties;
import org.gradoop.flink.io.impl.rdbms.tuples.PKTuple;

public class TupleToEdge extends RichMapFunction<Row,ImportEdge<String>> {
	private List<TablesToEdges> tables;
	private int tablePos;
	private TablesToEdges currentTable;
	
	public TupleToEdge(int tablePos){
		this.tablePos = tablePos;
	}

	@Override
	public ImportEdge<String> map(Row tuple) throws Exception {
		this.currentTable = tables.get(tablePos);
		ImportEdge<String> e = new ImportEdge<String>();
		e.setLabel(currentTable.getRelationshipType());
		e.setSourceId(tuple.getField(0).toString());
		e.setTargetId(tuple.getField(1).toString());
		e.setProperties(AttributesToProperties.getProperties(tuple, currentTable.getRowheader()));
		return e;
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
