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
		case "sqlserver":
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
