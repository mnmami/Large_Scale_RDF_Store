<%@ page import="com.fasterxml.jackson.annotation.JsonInclude.Include" %>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8" %>
<%@ taglib uri='http://java.sun.com/jsp/jstl/core' prefix='c' %>
<%@ page import="java.util.ArrayList" %>
<%@ page import="java.util.List" %>
<%@ page import="scala.Tuple2" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Mapping Phase</title>
<style type="text/css">
	.EntAttr tr:nth-child(even) { background: #F9F9F9; }
	.EntAttr tr:nth-child(odd) { background: #FFF; }
	.EntAttr { width: 100%; }
</style>
<script type="text/javascript">
	var selectedAttribute, selectedEntity;
	var json = new Object();
	json['mappings'] =  [];
	json['properties'] = [];
	
	function newECSameAsMap(entity) {
		alert('Select now a Class');
		window['selectedEntity'] = entity;
		$('#mappings').append('<br/><span>SameAs: [' + entity + ' -> <span id="classMap-' + entity + '">(Select a Class)</span>]</span>');
	}
	
	function newECSubClassOfMap(entity) {
		alert('Select now a Class'); 
		$('#mappings').append('<br/><span>SubClassOf: [' + entity + ' -> <span id="classMap-' + entity + '">(Select a Class)</span>]</span>');
	}
	
	function newAPSameAsMap(entity,attribute) {
		alert('Select now an Attribute');
		window['selectedAttribute'] = attribute;
		$('#classMap-' + entity).append('<br/><span style="padding:3em">SameAs: [' + attribute + ' -> <span id="propMap-' + attribute + '">(Select a Property)</span>]</span>');
	}
	
	function selectClass(val){
		$("#classMap-" + selectedEntity).text(val);
		var replaceColonByUnderscore = val.replace(":","-"); // "prefix:className" => ":" is now allowed in an id element
		
		// Start filling JSON mapping object
		json['ent'] = "Stop";
		json['mapTo'] = replaceColonByUnderscore;		
		$("#" + replaceColonByUnderscore + " option").each(function() {
			json['properties'].push(this.text); // Set the properties of the JSON object
		});
		$("span.Stop").each(function(index, obj) {
			json['properties'].push($(this).text()); // Set the properties of the JSON object
		});
		$(".property").not("#" + val.replace(':','-')).prop('disabled', 'disabled');
	}
	
	function selectProperty(val,entity){
		$('#propMap-' + selectedAttribute).text(val);
		var i = jQuery.inArray(selectedAttribute,json['properties']);
		if(i != -1) {
			json['properties'].splice(i, 1);
			var jsonStr = JSON.stringify(json);
		}
		//alert(selectedAttribute);
		json['mappings'].push(selectedAttribute + ">" + val);
	}
	
	function submit(){
		json['type'] = "csv";
		json['csvPath'] = "${csvPath}";
		json['semDataPath'] = "${semDataPath}";
		
		sessionStorage.setItem('user', JSON.stringify(json));
		window.location.replace("/SparkServlet/loading.jsp");
		
		//var jsonStr = JSON.stringify(json);
		//alert(jsonStr);

	}
</script>
<!-- Latest compiled and minified CSS -->
<link rel="stylesheet"
	href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css">

<!-- Optional theme -->
<link rel="stylesheet"
	href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap-theme.min.css">
</head>
<body>

	<jsp:include page="navbar.html" />

	<div class="container" style="width: 50%;">
		<table style="width: 100%; min-height: 400px; padding: 10px;">
			<tr>
				<td colspan="2"><h2 class="form-signin-heading">Non-semantic Data Source: CSV File</h2></td>
			</tr>
			<tr>
				<td style="width: 50%;"><b>Entity</b></td><td><b>Attributes</b></td>
			</tr>
			<tr>
				<td style="vertical-align: top;">
					${csvEntity}
					<div class="btn-group">
						<button class="btn btn-default btn-xs dropdown-toggle" type="button" data-toggle="dropdown" aria-expanded="false">
					    	New mapping <span class="caret"></span>
					 	</button>
						<ul class="dropdown-menu" role="menu">
							<li role="presentation"><a role="menuitem" onclick="newECSameAsMap('${csvEntity}');" tabindex="-1" href="#">Entity-Class: SameAs</a></li>
							<li role="presentation"><a role="menuitem" onclick="newECSubClassOfMap('${csvEntity}');" tabindex="-1" href="#">Entity-Class: SubClassOf</a></li>
						</ul>
					</div>
				</td>
				<td style="vertical-align: top;">
					<table>
						<c:forEach items="${csvAttributes}" var="csvAttribute">						
							<tr>
								<td><span class="${csvEntity}"><c:out value="${csvAttribute}" /></span></td>
								<td>
									<div class="btn-group">
										<button class="btn btn-default btn-xs dropdown-toggle" type="button" data-toggle="dropdown" aria-expanded="false">
									    	New mapping <span class="caret"></span>
									 	</button>
										<ul class="dropdown-menu" role="menu">
											<li role="presentation"><a role="menuitem" tabindex="-1" href="#" onclick="newAPSameAsMap('${csvEntity}','${csvAttribute}')">Attribute-Property: SameAs</a></li>
											<!--li role="presentation"><a role="menuitem" tabindex="-1" href="#">Attribute-Class: PartOf</a></li-->
										</ul>
									</div>
								</td>
							</tr>
						</c:forEach>
					</table>
				</td>
			</tr>
			<tr>
				<td colspan="2" style="border: 1px solid #F0F0F0; padding: 3px;">
					<b>Mapping rules:</b>
					<div id="mappings">
						
					</div>
					<button class="btn btn-primary btn-block" style="width: 200px; float: right;" onclick="submit();" type="button">Show Mapping description</button>
				</td>
			</tr>
			<tr>
				<td colspan="2"><h2 class="form-signin-heading">Semantic Data Source</h2></td>				
			</tr>
			<tr>		
				<table class='EntAttr'>
					<tr>
						<td><b>Class</b></td><td><b>Properties</b></td>
					</tr>
					<!--<tr>
						<td style='padding: 2px;'><span style='cursor: pointer; word-wrap: break-word;' onclick="selectClass('Stops');">Stops</span></td>
						<td>
							<select onchange="selectProperty(this.value);">
								<option>Name</option>
								<option>Description</option>
							</select>
					</td>-->
					<% List<Tuple2<String, Iterable<String>>> ntriples = (ArrayList<Tuple2<String, Iterable<String>>>) request.getAttribute("fromSemData");
					for (Tuple2<String, Iterable<String>> post : ntriples) {
						//String sclass = post._1().replace("<", "").replace(">", "");
						String pref_className = getShortenClass(post._1(), (String)request.getAttribute("dsName"));
						
						out.println("<tr><td style='padding: 2px; width: 50%;'><span style='cursor: pointer; word-wrap: break-word;' onclick=\"selectClass('" + pref_className + "')\">" + pref_className + "</span></td><td><select class='property' id='" + pref_className.replace(":", "-") + "' onchange='selectProperty(this.value);' style='width: 100%; background-color: #fff;'><option>Select a property</option>");
						for (String prop : post._2()) {
							String sprop = getShortenClass(prop, (String)request.getAttribute("dsName"));
							out.println("<option>" + sprop + "</option>");
						}
						out.println("</select></td>");
					} %>
					
					<%! public String getShortenClass(String classURI, String prefix){
						String[] partsOfURI = classURI.split("/");
						String className = partsOfURI[partsOfURI.length-1].replace(">", ""); // the last part is "className>"
						String pref_className = prefix + ":" + className;
						return pref_className;
					} %>
				</table>
			</tr>
		</table>
	</div>

	<!-- jQuery (necessary for Bootstrap's JavaScript plugins) -->
	<script
		src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.2/jquery.min.js"></script>
	<!-- Latest compiled and minified JavaScript -->
	<script
		src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/js/bootstrap.min.js"></script>
</body>
</html>