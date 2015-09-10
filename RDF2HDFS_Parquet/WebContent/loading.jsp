<%@ page import="com.fasterxml.jackson.annotation.JsonInclude.Include" %>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8" %>
<%@ taglib uri='http://java.sun.com/jsp/jstl/core' prefix='c' %>
<%@ page import="java.util.ArrayList" %>
<%@ page import="java.util.List" %>
<%@ page import="javax.servlet.http.HttpSession" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	<title>Mapping Phase</title>
	
	<style type="text/css"></style>
	<script>
	function startLoading(){
		
	}
	</script>	
	<script	src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.2/jquery.min.js"></script>			
	<script type="text/javascript">
	$(document).ready(function(){
		var obj = JSON.parse(sessionStorage.getItem('user'));
		//alert(obj['mappings']);

		// Unified Table
		var row = "";
		var table = "<strong>Table:</strong>"
		table += "<table border='1' style='text-align: center;'><tr>";
		for (i = 1; i < obj['properties'].length; i++) {
			table += "<td style='padding-left: 3px; padding-right: 3px;'>" + obj['properties'][i] + "</td>";
			row += "<td> ... </td>";
		}
		table += "</tr><tr style='height: 30px;'> " + row + "</tr></table>";
		$("#uTable").html(table);
		
		// Mappings
		// Entity sameAs Class
		var mappings = "<strong>List of mappings:</strong><br/>";
		var ent = obj['ent']; 
		var cls = obj['mapTo'];		
		mappings += ent + " > " + cls;	
		mappings += "<ul>";
		for (i = 0; i < obj['mappings'].length; i++) {
			mappings += "<li>" + obj['mappings'][i].replace(">", " > ") + "</li>";
		}
		mappings += "</ul>";
		$("#mappings").html(mappings);
		
		// Start loading
		$("#startLoading").click(function() {
			$.ajax({
				type:'POST',
				charset:'utf-8',
				//dataType: 'JSON',
				url:'/SparkServlet/Loading',
				data: {					
					semDataPath: obj['semDataPath'],
					csvPath: json['csvPath'],
					ent: obj['ent'],
					cls: obj['mapTo'],
					mappings: obj['mappings'],
					properties: obj['properties']
				},
				success:function(msg)
				{
				    console.log(msg);
				    //window.location.replace("/SparkServlet/Loading");
				},
				error:function(xhr,status)
				{
				    console.log(status);
				}     
				//contentType:"application/json"  
			});
		});		
	});
	</script>
	
	<!-- Latest compiled and minified CSS -->
	<link rel="stylesheet"href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css">
	<!-- Optional theme -->
	<link rel="stylesheet"	href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap-theme.min.css">
</head>
<body>

	<jsp:include page="navbar.html" />

	<div class="container" style="width: 80%;">
		<h2 class="form-signin-heading">Loading</h2>
		
		<div id="mappings" style="overflow: scroll; overflow: auto;"></div>
		
		<div id="uTable" style="overflow: scroll; overflow: auto;"></div>
		
		<div style="margin-top: 50px;">
			<strong>Control:</strong><br/>
			<label><input type="checkbox" name="keepProv" value="yes" /> Keep provenance: add prefix of dataset source to each tuple</label>
		</div>
		
		<button class="btn btn-primary btn-block" id="startLoading" style="width: 150px; float: right;" type="button">Load data</button>
	</div>
		
	<!-- Latest compiled and minified JavaScript -->
	<script
		src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/js/bootstrap.min.js"></script>
</body>
</html>