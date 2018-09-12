package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;

public class CleanMapper extends RichFlatMapFunction<TableToNode, Vertex> {
	List<Vertex> vertices;
	EPGMVertexFactory vertexFactory;

	public CleanMapper(EPGMVertexFactory vertexFactory) {
		this.vertexFactory = vertexFactory;
	}
	
	@Override
	public void flatMap(TableToNode table, Collector<Vertex> out) throws Exception {
		// used to find foreign key properties
		ArrayList<String> fkProps = new ArrayList<String>();

		for (FkTuple fk : table.getForeignKeys()) {
			fkProps.add(fk.f0);
		}
		
		for(Vertex v : vertices) {
			Properties newProps = new Properties();
			for(Property prop : v.getProperties()){
				if(!fkProps.contains(prop.getKey()) && !prop.getKey().equals(RdbmsConstants.PK_ID)){
					newProps.set(prop);
				}
			}
			out.collect((Vertex) vertexFactory.initVertex(v.getId(),v.getLabel(),newProps));
		}

	}

	public void open(Configuration parameters) throws Exception {
		this.vertices = getRuntimeContext().getBroadcastVariable("vertices");
	}
}
