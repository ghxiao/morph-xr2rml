package fr.unice.i3s.morph.xr2rml.server

import org.apache.log4j.Logger
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.glassfish.jersey.servlet.ServletContainer

import es.upm.fi.dia.oeg.morph.base.MorphProperties
import fr.unice.i3s.morph.xr2rml.service.SparqlrestService

/**
 * Servlet container based on Jetty embedded server to run the SPARQL endpoint
 */
object SparqlEndpoint {

    val logger = Logger.getLogger(this.getClass)

    def create(properties: MorphProperties) {
        val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
        context.setContextPath("/")

        val server = new Server(properties.serverPort)
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
    }
}