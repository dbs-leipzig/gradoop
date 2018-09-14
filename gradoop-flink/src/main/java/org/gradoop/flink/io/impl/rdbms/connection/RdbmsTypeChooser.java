package org.gradoop.flink.io.impl.rdbms.connection;

import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;

/**
 * Manages assigning of relational database management type
 */
public class RdbmsTypeChooser {

	/**
	 * Assigns connected database with management type
	 * 
	 * @param rdbms
	 *            Name of datanase management system
	 * @return Database management system type
	 */
	public static int choose(String rdbms) {

		int rdbmsType;

		switch (rdbms) {

		case "posrgresql":
		case "mysql":
		case "h2":
		case "sqlite":
		case "hsqldb":
		default:
			rdbmsType = RdbmsConstants.MYSQL_TYPE_ID;
			break;

		case "derby":
		case "microsoft sql server":
		case "oracle":
			rdbmsType = RdbmsConstants.SQLSERVER_TYPE_ID;
			break;
		}

		return rdbmsType;
	}
}
