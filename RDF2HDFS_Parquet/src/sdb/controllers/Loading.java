package sdb.controllers;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import sbd.model.Triple;

/**
 * Loading implementation class servlet
 */
@WebServlet(urlPatterns = { "/Loading" })
public class Loading extends HttpServlet {

	protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		String semDataPath = request.getParameter("semDataPath");
		String csvPath = request.getParameter("csvPath");
		String ent = request.getParameter("ent");
		String mapTo = request.getParameter("mapTo");
		String mappings = request.getParameter("mappings");
		String properties = request.getParameter("properties");

		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[2]").set("spark.executor.memory", "1g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(ctx);

		// CSV files
		JavaRDD<String> lines = ctx.textFile(csvPath);

		// Map text lines to Triple objects
		JavaRDD<Triple> triples = lines.map(s -> call(s));

		// get properties
		// create parquet table

		// read nt file (path)

		// read csv (path)

		request.getRequestDispatcher("/loading.jsp").forward(request, response);

	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		processRequest(request, response);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		processRequest(request, response);
	}

	@Override
	public String getServletInfo() {
		return "Short description";
	}

	// Helping methods
	private Triple call(String line) {
		String[] parts = line.split(" ");

		Triple triple = null;
		if (parts[1].equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"))
			triple = new Triple(parts[0], null, parts[2]);
		else
			triple = new Triple(parts[0], parts[1], parts[2]);
		return triple;
	}

}
