package sdb.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import sbd.classes.SchemaExtractor;
import scala.Tuple2;

/**
 * Mapping implementation class servlet
 */
@WebServlet(urlPatterns = { "/Mapping" })
public class Mapping extends HttpServlet {

	protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, ClassNotFoundException {
		String semDataPath = "/home/mohamed/Documents/SBD_Input/" + request.getParameter("semDataPath");
		String semSchemaPath = "/home/mohamed/Documents/SBD_Input/" + request.getParameter("semSchemaPath");
		String csvPath = "/home/mohamed/Documents/SBD_Input/" + request.getParameter("csvPath");
		String csvEntity = request.getParameter("csvEntity");
		String dsName = request.getParameter("dsName");

		SchemaExtractor se = new SchemaExtractor();
		ArrayList<String> csvHeader = se.fromCSV(csvPath, ",");
		ArrayList<Tuple2<String, Iterable<String>>> fromSemData = (ArrayList<Tuple2<String, Iterable<String>>>) se.fromSemData(semDataPath, " ", dsName);
		//se.CSVtoParquet("/home/mohamed/Documents/SBD_Output/Tables/");
		
		//se.JSONtoParquet("/home/mohamed/Documents/SBD_Output/Schemas/","/home/mohamed/Documents/SBD_Output/Tables/", null);
		//ArrayList<Tuple2<String, Iterable<Tuple2<String, String>>>> fromSemSchema = (ArrayList<Tuple2<String, Iterable<Tuple2<String, String>>>>) se.fromSemSchema(semSchemaPath, " ");
		
		request.setAttribute("csvAttributes", csvHeader);
		request.setAttribute("csvEntity", csvEntity);
		//request.setAttribute("fromSemData", fromSemData);
		//request.setAttribute("fromSemSchema", fromSemSchema);
		request.setAttribute("dsName", dsName); // Dataset name
		
		// Paths of input files
		request.setAttribute("csvPath", csvPath);
		request.setAttribute("semDataPath", semDataPath);
		
		//request.getRequestDispatcher("/mapping.jsp").forward(request, response);
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			processRequest(request, response);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			processRequest(request, response);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public String getServletInfo() {
		return "Short description";
	}

}
