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
