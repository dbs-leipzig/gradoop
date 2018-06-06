package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

public class PkId extends Tuple2<String,GradoopId>{
	
	public PkId(){}
	
	public PkId(String primaryKey,GradoopId id){
		this.f0 = primaryKey;
		this.f1 = id;
		
	}

	public String getPrimaryKey() {
		return f0;
	}

	public void setPrimaryKey(String primaryKey) {
		this.f0 = primaryKey;
	}

	public GradoopId getId() {
		return f1;
	}

	public void setId(GradoopId id) {
		this.f1 = id;
	}
}
