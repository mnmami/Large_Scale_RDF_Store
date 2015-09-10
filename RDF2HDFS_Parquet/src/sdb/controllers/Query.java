package sdb.controllers;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.api.java.function.Function;

import sbd.model.Triple;

/**
 * Servlet implementation class servlet
 */
@WebServlet(urlPatterns = { "/Query" })
public class Query extends HttpServlet {

	protected void processRequest(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html;charset=UTF-8");

		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL")
				.setMaster("local[2]").set("spark.executor.memory", "1g");

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(ctx);

		// 1. Load a text file and convert each line to a Java Bean.
		@SuppressWarnings("serial")
		JavaRDD<Triple> triples = ctx.textFile("/home/mohamed/Documents/SBD_Input/2014-09-09-HistoricThing.node.sorted.nt").map(
				new Function<String, Triple>() {

					@Override
					public Triple call(String line) {
						String[] parts = line.split(" ");

						Triple triple = new Triple(parts[0], parts[1], parts[2]);
						return triple;
					}
				});
		

		PrintWriter out = response.getWriter();
		List<String> stops_nodes = triples.map(new Function<Triple, String>() {
			@Override
			public String call(Triple t1) throws Exception {
				return "<br/>Stop: " + t1.subject + "," + t1.property + ","
						+ t1.object;
			}
		}).collect();
		for (String name : stops_nodes) {
			out.println(name);
		}

		ctx.close();
	}

	@Override
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		processRequest(request, response);
	}

	@Override
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		processRequest(request, response);
	}

	@Override
	public String getServletInfo() {
		return "Short description";
	}

}
