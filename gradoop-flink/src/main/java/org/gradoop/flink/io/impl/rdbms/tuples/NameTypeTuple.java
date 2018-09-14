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

package org.gradoop.flink.io.impl.rdbms.tuples;

import java.sql.JDBCType;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Tuple representing a key string and belonging data type pair
 */
public class NameTypeTuple extends Tuple2<String, JDBCType> {

	private static final long serialVersionUID = 1L;

	/**
	 * Key string
	 */
	private String name;

	/**
	 * JDBC data type
	 */
	private JDBCType type;

	public NameTypeTuple() {
	}

	/**
	 * Constructor
	 * 
	 * @param name
	 *            Key string
	 * @param type
	 *            JDBC data type
	 */
	public NameTypeTuple(String name, JDBCType type) {
		this.name = name;
		this.f0 = name;
		this.type = type;
		this.f1 = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public JDBCType getType() {
		return type;
	}

	public void setType(JDBCType type) {
		this.type = type;
	}
}
