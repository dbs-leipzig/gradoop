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

import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

/**
 * Parses database tuples to valid EPGM properties
 */
public class AttributesToProperties {

	//parses properties with foreign key attributes included
	public static Properties getProperties(Row tuple, RowHeader rowheader) {
		Properties props = new Properties();
		for (RowHeaderTuple rht : rowheader.getRowHeader()) {
			try{
				props.set(rht.getName(), PropertyValueParser.parse((tuple.getField((rht.getPos())))));
			}catch(Exception e){
				System.err.println("Properties require non null.");
			}
		}
		return props;
	}

	//parses properties without foreign key attributes
	public static Properties getPropertiesWithoutFKs(Row tuple, RowHeader rowheader) {
		Properties props = new Properties();
		for (RowHeaderTuple rht : rowheader.getRowHeader()) {
			if(!rht.getAttType().equals(RdbmsConstants.FK_FIELD)){
				props.set(rht.getName(), PropertyValueParser.parse((tuple.getField((rht.getPos())))));
			}
		}
		return props;
	}
}
