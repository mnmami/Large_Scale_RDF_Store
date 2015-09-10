<%@page import="com.fasterxml.jackson.annotation.JsonInclude.Include"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Mapping</title>

<!-- Latest compiled and minified CSS -->
<link rel="stylesheet"
	href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css">

<!-- Optional theme -->
<link rel="stylesheet"
	href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap-theme.min.css">
</head>
<body>
	<jsp:include page="navbar.html" />

	<div class="container" style="width: 30%;">

		<form class="form-signin" action="Mapping" method="post">
			<h2 class="form-signin-heading">Semantic data source</h2>
			<div>
				<label for="semSrc">Semantic Data:</label> <input
					type="file" id="semSrc" class="form-control" name="semDataPath" required
					autofocus />
					
				<label for="semSrc">Semantic Schema:</label> <input
				type="file" id="semSrc" class="form-control" name="semSchemaPath" required
				autofocus />
				<label>Give a short name to the dataset: <input type="text" class="form-control" name="dsName" /></label>
			</div>
			
			<h2 class="form-signin-heading">Non-semantic data source</h2>
			<div class="dropdown">
				<button class="btn btn-default dropdown-toggle" type="button"
					id="dropdownMenu1" data-toggle="dropdown" aria-expanded="true">
					Dropdown <span class="caret"></span>
				</button>
				<ul class="dropdown-menu" role="menu"
					aria-labelledby="dropdownMenu1">
					<li role="presentation"><a role="menuitem" tabindex="-1"
						href="#" onclick="$('#uploadCSV').removeClass('hide');">CSV
							file</a></li>
					<li role="presentation"><a role="menuitem" tabindex="-1"
						href="#" onclick="$('#uploadCSV').addClass('hide');">Relational</a></li>
					<li role="presentation"><a role="menuitem" tabindex="-1"
						href="#">JSON</a></li>
				</ul>				
			</div>
			<label for="semSrc" class="sr-only">Non-semantic data source:</label> <input
				type="file" id="uploadCSV" class="form-control" name="csvPath"
				required autofocus />
				<label>Give a name to the Entitry represented by this CSV file: <input type="text" class="form-control" name="csvEntity" /></label>
			<br />
			<button class="btn btn-primary btn-block" type="submit">Extract
				Schema</button>
		</form>

	</div>

	<!-- jQuery (necessary for Bootstrap's JavaScript plugins) -->
	<script
		src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.2/jquery.min.js"></script>
	<!-- Latest compiled and minified JavaScript -->
	<script
		src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/js/bootstrap.min.js"></script>
</body>
</html>