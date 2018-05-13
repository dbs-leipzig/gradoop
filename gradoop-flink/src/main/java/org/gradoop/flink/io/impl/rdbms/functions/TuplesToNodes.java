package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.epgm.LogicalGraphFactory;

public class TuplesToNodes {
	public TuplesToNodes(){
	}
	
	public static LogicalGraph getToNodesGraph(ExecutionEnvironment env, LogicalGraphFactory lgf,ArrayList<String> toNodes){
		
		DataSet<Row> dbData =
			    
		return null;
//		return lgf.fromCollections(vertices, edges);
	}
}
