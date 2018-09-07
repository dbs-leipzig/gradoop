package org.gradoop.flink.io.impl.rdbms.connection;

import java.util.regex.Pattern;

/**
 * Chooses a fitting pageination prepared statement depending on connected managementsystem
 *
 */
public class PageinationQueryChooser {
	
	/**
	 * Chooses a fitting pageination prepared statement depending on connected managementsystem
	 * @param rdbms Name of connected managementsystem
	 * @return	Valid sql pageination prepared statement
	 */
	public static String choose(String rdbms){
		String pageinationQuery = "";
		
		switch(rdbms){
		case "posrgresql" : 
		case "mysql" :
		case "h2" :
		case "sqlite" :
		case "hsqldb" : 
		default:
			pageinationQuery = " LIMIT ? OFFSET ?";
			break;
		case "derby":
		case "sqlserver":
		case "oracle":
			pageinationQuery = " ORDER BY 1 OFFSET (?) ROWS FETCH NEXT (?) ROWS ONLY";
			break;
		}
		return pageinationQuery;
	}
}
