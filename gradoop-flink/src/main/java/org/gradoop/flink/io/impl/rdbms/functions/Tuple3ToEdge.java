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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

/**
 * Creates edges from joined foreign key sets
 */
public class Tuple3ToEdge implements MapFunction<Tuple2<Tuple3<GradoopId,String,Properties>,IdKeyTuple>,Edge> {
	
	/**
	 * Name of converted table
	 */
	String tableName;
	
	/**
	 * Constructor
	 * @param tableName Name of converted table
	 */
	public Tuple3ToEdge(String tableName){
		this.tableName = tableName;
	}
	
	@Override
	public Edge map(Tuple2<Tuple3<GradoopId, String, Properties>, IdKeyTuple> value) throws Exception {
		Edge e = new Edge();
		e.setId(GradoopId.get());
		e.setSourceId(value.f0.f0);
		e.setTargetId(value.f1.f0);
		e.setProperties(value.f0.f2);
		e.setLabel(tableName);
		return e;
	}
}
