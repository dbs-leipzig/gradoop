package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.SchemaType;

public class ParsePKIDWithProperties extends RichMapFunction<Row,Tuple3<String,GradoopId,Properties>> {
	int tablePos;
	int fkPos;
	List<RDBMSTable> tables;
	
	public ParsePKIDWithProperties(int tablePos, int fkPos){
		this.tablePos = tablePos;
		this.fkPos = fkPos;
	}
	
	@Override
	public Tuple3<String, GradoopId, Properties> map(Row row) throws Exception {
		// TODO Auto-generated method stub
		Properties properties = new Properties();
		for(SchemaType prop : tables.get(tablePos).getSimpleAttributes()){
			properties.set(prop.getName(), row.getField(prop.getPos()).toString());;
		}
		
		return new Tuple3(row.getField(fkPos).toString(),row.getField(row.getArity()-1),properties);
	}
	
	public void open(Configuration parameters) throws Exception {
		// super.open(parameters);
		this.tables = getRuntimeContext().getBroadcastVariable("tables");
	}
}
