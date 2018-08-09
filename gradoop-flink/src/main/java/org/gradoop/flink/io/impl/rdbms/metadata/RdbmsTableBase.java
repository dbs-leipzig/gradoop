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
package org.gradoop.flink.io.impl.rdbms.metadata;

import java.util.ArrayList;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;

/**
 * Represents the relational database schema
 */
public class RdbmsTableBase {
	
	/**
	 * Name of database table
	 */
	private String tableName;
	
	/**
	 * List of primary keys of database table
	 */
	private ArrayList<NameTypeTuple> primaryKeys;
	
	/**
	 * List of foreign key of database table
	 */
	private ArrayList<FkTuple> foreignKeys;
	
	/**
	 * List of further attributes (no primary, foreign key attributes) of database table
	 */
	private ArrayList<NameTypeTuple> furtherAttributes;
	
	/**
	 * Number of rows of table
	 */
	private int rowCount;

	/**
	 * Constructor
	 * @param tableName Name of database table
	 * @param primaryKeys List of primary keys
	 * @param foreignKeys List of foreign keys
	 * @param furtherAttributes List of further attributes
	 * @param rowCount Number of rows
	 */
	public RdbmsTableBase(String tableName, ArrayList<NameTypeTuple> primaryKeys,
			ArrayList<FkTuple> foreignKeys, ArrayList<NameTypeTuple> furtherAttributes, int rowCount) {
		this.tableName = tableName;
		this.primaryKeys = primaryKeys;
		this.foreignKeys = foreignKeys;
		this.furtherAttributes = furtherAttributes;
		this.rowCount = rowCount;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ArrayList<NameTypeTuple> getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(ArrayList<NameTypeTuple> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public ArrayList<FkTuple> getForeignKeys() {
		return foreignKeys;
	}

	public void setForeignKeys(ArrayList<FkTuple> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}

	public ArrayList<NameTypeTuple> getFurtherAttributes() {
		return furtherAttributes;
	}

	public void setFurtherAttributes(ArrayList<NameTypeTuple> furtherAttributes) {
		this.furtherAttributes = furtherAttributes;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}
}
