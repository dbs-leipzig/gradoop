package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;

/**
 * Assigns new vertex properties, ignoring primary and foreign key attributes
 */
public class DeletePKandFKs extends RichMapFunction<Vertex,Vertex> {
	
	/**
	 * List of foreign key properties
	 */
	ArrayList<String> fkProps;
	
	/**
	 * Constructor
	 * @param fkProps List or foreign key properties
	 */
	public DeletePKandFKs(ArrayList<String> fkProps){
		this.fkProps = fkProps;
	}
	
	@Override
	public Vertex map(Vertex v1) throws Exception {
		Properties newProps = new Properties();
		for(Property prop : v1.getProperties()){
			if(!fkProps.contains(prop.getKey()) && !prop.getKey().equals(RDBMSConstants.PK_ID)){
				newProps.set(prop);
			}
		}
		v1.setProperties(newProps);
		return v1;
	}
}
