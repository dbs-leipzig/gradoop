package org.gradoop.flink.io.impl.rdbms.functions;

import java.sql.JDBCType;
import java.util.ArrayList;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.types.Row;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeader;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

/**
 * class to create a concatenated string of primary key attribute values
 * 
 * @author pc
 *
 */
public class PrimaryKeyConcatString {
	
	public static String getPrimaryKeyString(Row tuple, RowHeader rowHeader) {
		String pkString = "";

		for (RowHeaderTuple rht : rowHeader.getRowHeader()) {
			if (rht.getAttType().equals(RDBMSConstants.PK_FIELD)) {
				pkString += tuple.getField(rht.getPos()).toString();
			}
		}
		return pkString;
	}
}
