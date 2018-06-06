package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.tuples.PkId;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.SchemaType;

public class ToNodesPkId extends RichMapFunction<Row, PkId> {

	int tablePos;
	int fkPos;
	List<RDBMSTable> tables;
	
	public ToNodesPkId(int tablePos, int fkPos){
		this.tablePos = tablePos;
		this.fkPos = fkPos;
	}
	
	@Override
	public PkId map(Row row) throws Exception {
		// TODO Auto-generated method stub
		
		return new PkId(row.getField(fkPos).toString(),(GradoopId) row.getField(row.getArity()-1));
	}
	
	public void open(Configuration parameters) throws Exception {
		// super.open(parameters);
		this.tables = getRuntimeContext().getBroadcastVariable("tables");
	}
}

