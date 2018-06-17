package com.apiserv;

import java.io.IOException;
import java.sql.SQLException;
import org.apache.jcs.access.exception.CacheException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;

import com.app.Hive;
  
public class Servico extends HttpServlet {

	private static Hive hiveClass = new Hive();

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			resp.setContentType("application/json;charset=utf-8");
			resp.setStatus(HttpStatus.OK_200);
			String userQuery = req.getParameter("q");
			resp.getWriter().println(hiveClass.jsonData(userQuery));
		} catch (SQLException | ClassNotFoundException | CacheException e) {
			resp.setContentType("application/json;charset=utf-8");
			resp.setStatus(HttpStatus.BAD_REQUEST_400);
			resp.getWriter().println("{\"error\":1}");
		}
	}
}