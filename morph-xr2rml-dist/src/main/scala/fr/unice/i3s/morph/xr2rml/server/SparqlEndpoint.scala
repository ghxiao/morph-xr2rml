package fr.unice.i3s.morph.xr2rml.server

import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.glassfish.jersey.servlet.ServletContainer
import fr.unice.i3s.morph.xr2rml.service.SparqlrestService

object SparqlEndpoint {

    val log4jfile = this.getClass.getClassLoader.getResource("xr2rml-log4j.properties")
    println("Loading log4j configuration: " + log4jfile)
    PropertyConfigurator.configure(log4jfile)
    
    val logger = Logger.getLogger(this.getClass)

    def main(args: Array[String]) {
        try {
            val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
            context.setContextPath("/")
            val server = new Server(8080)
            server.setHandler(context)

            val servlet = new ServletHolder(classOf[ServletContainer])
            //servlet.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig")
            //servlet.setInitParameter("com.sun.jersey.config.property.packages", "fr.unice.i3s.morph.xr2rml.service")
            servlet.setInitParameter("jersey.config.server.provider.classnames", classOf[SparqlrestService].getCanonicalName);
            servlet.setInitParameter("requestBufferSize", "8192")
            servlet.setInitParameter("headerBufferSize", "8192")
            context.addServlet(servlet, "/*")

            server.start()
            server.join()
        } catch {
            case e: Exception => {
                logger.fatal("An unexpected error occured: " + e.getMessage())
                e.printStackTrace()
                System.exit(-1)
            }
        }
    }
}