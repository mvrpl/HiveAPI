package com.mvrpl;

import org.eclipse.jetty.server.Server; 
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.apiserv.Servico;

public class App {

    public static void main( String[] args ) {
      try {
        Server server=new Server(9080);
        ServletContextHandler handler = new ServletContextHandler(server, "/api");
        handler.addServlet(Servico.class, "/");
        server.start();
        server.join();
      } catch (Exception e) {
          System.out.println(e.getMessage());
      }
    }
}
