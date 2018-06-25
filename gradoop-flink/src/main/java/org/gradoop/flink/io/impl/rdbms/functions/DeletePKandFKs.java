package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;

public class DeletePKandFKs extends RichMapFunction<Vertex,Vertex> {
	int tPos;
	ArrayList<String> fkProps;
	
	public DeletePKandFKs(ArrayList<String> fkProps){
		this.tPos = tPos;
		this.fkProps = fkProps;
	}
	@Override
	public Vertex map(Vertex v1) throws Exception {
		// TODO Auto-generated method stub
		Properties newProps = new Properties();
		for(Property prop : v1.getProperties()){
			if(!fkProps.contains(prop.getKey()) && !prop.getKey().equals("PrimaryKey")){
				newProps.set(prop);
			}
		}
		v1.setProperties(newProps);
		return v1;
	}
}
