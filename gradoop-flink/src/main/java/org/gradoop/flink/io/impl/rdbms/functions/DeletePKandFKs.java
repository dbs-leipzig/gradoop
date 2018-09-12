/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;

/**
 * Assigns new vertex properties, ignoring primary and foreign key attributes
 */
public class DeletePKandFKs extends RichMapFunction<Vertex,Vertex> {
	
	/**
	 * List of foreign key properties
	 */
	ArrayList<String> fkProps;
	EPGMVertexFactory vertexFactory;
	
	/**
	 * Constructor
	 * @param fkProps List or foreign key properties
	 */
	public DeletePKandFKs(EPGMVertexFactory vertexFactory, ArrayList<String> fkProps){
		this.fkProps = fkProps;
		this.vertexFactory = vertexFactory;
	}
	
	@Override
	public Vertex map(Vertex v) throws Exception {
		Properties newProps = new Properties();
		for(Property prop : v.getProperties()){
			if(!fkProps.contains(prop.getKey()) && !prop.getKey().equals(RdbmsConstants.PK_ID)){
				newProps.set(prop);
			}
		}
		return (Vertex) vertexFactory.initVertex(v.getId(),v.getLabel(),newProps);
	}
}
