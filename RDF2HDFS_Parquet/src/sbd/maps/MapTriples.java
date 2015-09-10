package sbd.maps;

import org.apache.spark.api.java.function.Function;

import sbd.model.Triple;

@SuppressWarnings("serial")
public class MapTriples implements Function<String, Triple> {

	public String fdel;

	public MapTriples(String del) {
		this.fdel = del;
	}

	@Override
	public Triple call(String line) {
		String[] parts = line.split(fdel);

		Triple triple = new Triple(parts[0], parts[1], parts[2]);
		return triple;
	}
}