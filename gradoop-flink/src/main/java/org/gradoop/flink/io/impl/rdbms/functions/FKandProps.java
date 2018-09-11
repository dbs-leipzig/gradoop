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

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;

/**
 * Creates a tuple of foreign key one, foreign key two and belonging properties from row 
 */
public class FKandProps extends RichMapFunction<Row, Tuple3<String, String, Properties>> {
	
	/**
	 * List of all instances going to convert to edges
	 */
	private List<TableToEdge> tables;
	
	/**
	 * Current Position of iteration
	 */
	private int tablePos;
	
	/**
	 * Current table
	 */
	private TableToEdge currentTable;
	
	/**
	 * Current rowheader
	 */
	private RowHeader rowheader;

	/**
	 * Constructor
	 * @param tablePos Current position of iteration
	 */
	public FKandProps(int tablePos) {
		this.tablePos = tablePos;
	}

	@Override
	public Tuple3<String, String, Properties> map(Row tuple) throws Exception {
		this.currentTable = tables.get(tablePos);
		this.rowheader = currentTable.getRowheader();
		
		Tuple3<String,String,Properties> fkPropsTuple = new Tuple3("","",new Properties());
		try {
		 fkPropsTuple = new Tuple3<String,String,Properties>(tuple.getField(rowheader.getForeignKeyHeader().get(0).getPos()).toString(),
				tuple.getField(rowheader.getForeignKeyHeader().get(1).getPos()).toString(),
				AttributesToProperties.getPropertiesWithoutFKs(tuple, rowheader));
		}catch(Exception e) {}
		return fkPropsTuple;
	}

	public void open(Configuration parameters) throws Exception {
		this.tables = getRuntimeContext().getBroadcastVariable("tables");
	}
}
