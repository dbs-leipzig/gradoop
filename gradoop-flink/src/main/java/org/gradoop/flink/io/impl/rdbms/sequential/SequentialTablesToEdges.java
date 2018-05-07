package org.gradoop.flink.io.impl.rdbms.sequential;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;

public class SequentialTablesToEdges {

	SequentialTablesToEdges() {

	}

	public static ArrayList<String> getTablesToEdges(ArrayList<RDBMSTable> tables) {
		ArrayList<String> tablesToEdges = new ArrayList<String>();
		HashSet<String> referencedTables = new HashSet<String>();

		for (RDBMSTable table : tables) {
			if (table.getForeignKeys() != null) {
				Iterator it = table.getForeignKeys().entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					referencedTables.add((String) pair.getValue());
				}
			}
		}

		for (RDBMSTable table : tables) {
			if (table.getForeignKeys() != null) {
				if (!referencedTables.contains(table.getTableName()) && table.getForeignKeys().size() == 2) {
					tablesToEdges.add(table.getTableName());
				}
			}
		}
		return tablesToEdges;
	}
}
