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

package org.gradoop.flink.io.impl.rdbms.connection;

import java.io.Serializable;

public class ParametersChooser {
	public static Serializable[][] choose(String rdbms, int parallelism, int rowCount) {
		Serializable[][] parameters;

		// splits database data in parts of same
		int partitionNumber;
		int partitionRest;

		if (rowCount < parallelism) {
			partitionNumber = 1;
			partitionRest = 0;
			parameters = new Integer[rowCount][2];
		} else {
			partitionNumber = rowCount / parallelism;
			partitionRest = rowCount % parallelism;
			parameters = new Integer[parallelism][2];
		}
		int j = 0;

		switch (rdbms) {
		case "posrgresql":
		case "mysql":
		case "h2":
		case "sqlite":
		case "hsqldb":
		default:
			for (int i = 0; i < parameters.length; i++) {
				if (i == parameters.length - 1) {
					parameters[i] = new Integer[] { partitionNumber + partitionRest, j };
				} else {
					parameters[i] = new Integer[] { partitionNumber, j };
					j = j + partitionNumber;
				}
			}
			break;
		case "derby":
		case "microsoft sql server":
		case "oracle":
			for (int i = 0; i < parameters.length; i++) {
				if (i == parameters.length - 1) {
					parameters[i] = new Integer[] { j, partitionNumber + partitionRest };
				} else {
					parameters[i] = new Integer[] { j, partitionNumber };
					j = j + partitionNumber;
				}
			}
			break;
		}
		return parameters;
	}
}
