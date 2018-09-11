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

import org.apache.flink.types.Row;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

/**
 * Concatenates multiple primary keys
 */
public class PrimaryKeyConcatString {
	/**
	 * Concatenates multiple primary keys
	 * @param tuple Tuple of a database relation
	 * @param rowheader Tuple belonging rowheader
	 * @return Concatenated primary key string
	 */
	public static String getPrimaryKeyString(Row tuple, RowHeader rowheader) {
		String pkString = "";
		for (RowHeaderTuple rht : rowheader.getRowHeader()) {
			if (rht.getAttType().equals(RdbmsConstants.PK_FIELD)) {
				pkString += tuple.getField(rht.getPos()).toString();
			}
		}
		return pkString;
	}
}
