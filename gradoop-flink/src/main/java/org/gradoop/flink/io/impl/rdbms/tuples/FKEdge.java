package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

public class FKEdge extends Tuple2<String,GradoopId>{
	String lable;
	GradoopId id;
	
	public FKEdge(String lable, GradoopId id){
		this.lable = lable;
		this.f0 = lable;
		this.id = id;
		this.f1 = id;
	}
	public FKEdge(){
	}
	
	public String getLable() {
		return lable;
	}
	public void setLable(String lable) {
		this.lable = lable;
	}
	public GradoopId getId() {
		return id;
	}
	public void setId(GradoopId id) {
		this.id = id;
	}
}
