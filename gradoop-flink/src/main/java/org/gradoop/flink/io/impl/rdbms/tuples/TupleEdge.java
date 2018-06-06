package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

public class TupleEdge extends Tuple3<String,GradoopId,Properties>{
	String lable;
	GradoopId id;
	Properties props;
	
	public TupleEdge(String lable, GradoopId id, Properties props){
		this.lable =  lable;
		this.f0 = lable;
		this.id = id;
		this.f1 = id;
		this.props = props;
		this.f2 = props;
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

	public Properties getProps() {
		return props;
	}

	public void setProps(Properties props) {
		this.props = props;
	}
}
