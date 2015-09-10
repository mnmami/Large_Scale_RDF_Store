package sbd.maps;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

@SuppressWarnings("serial")
public class MapSubjects implements Function<Row, String> {
	@Override
	public String call(Row row) {
		return "Subject: " + row.getString(0);
	}
}