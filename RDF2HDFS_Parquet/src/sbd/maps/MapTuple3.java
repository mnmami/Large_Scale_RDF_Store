package sbd.maps;

import java.util.Arrays;

import org.apache.spark.api.java.function.Function;

import sbd.model.Triple;

@SuppressWarnings("serial")
public class MapTuple3 implements Function<String, Triple> {

	public String fdel;

	public MapTuple3(String del) {
		this.fdel = del;
	}

	@Override
	public Triple call(String line) {

		String[] parts = line.split(fdel);

		Triple triple = new Triple(parts[0], parts[1], parts[2]);		
		return (Triple) Arrays.asList(triple);
	}
}