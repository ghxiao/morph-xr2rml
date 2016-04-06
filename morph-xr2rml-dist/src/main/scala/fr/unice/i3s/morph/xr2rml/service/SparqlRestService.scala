package fr.unice.i3s.morph.xr2rml.service

import org.apache.log4j.Logger

import com.hp.hpl.jena.query.QueryFactory

import fr.unice.i3s.morph.xr2rml.engine.MorphRunner
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.HttpHeaders
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status
import java.io.InputStreamReader
import java.io.FileInputStream
import javax.ws.rs.Consumes

/**
 * REST service implementing the SPARQL query protocol for queries SELECT, DESCRIBE and CONSTRUCT
 */
@Path("/sparql")
class SparqlrestService {

    val headerAccept = "Access-Control-Allow-Origin"

    val logger = Logger.getLogger(this.getClass)

    /**
     * Simple test service
     */
    @GET
    @Path("test")
    @Produces(Array("text/plain"))
    def test: Response = {
        return Response.status(Status.OK).header(headerAccept, "*").entity("SPARQL REST service is up and running").build
    }

    /**
     * Processing of SPARQL queries SELECT, DESCRIBE and CONSTRUCT
     */
    @GET
    @Consumes(Array("application/sparql-query"))
    def processSparqlQuery(@QueryParam("query") query: String,
                            @QueryParam("default-graph-uri") defaultGraphUris: java.util.List[String],
                            @QueryParam("named-graph-uri") namedGraphUris: java.util.List[String]): Response = {

        if (logger.isDebugEnabled) {
            logger.debug("GET XML, SPARQL query: " + query)
            logger.debug("GET XML, default graph: " + defaultGraphUris)
            logger.debug("GET XML, named graph: " + namedGraphUris)
        }

        try {
            if (query == null || query.isEmpty)
                return Response.status(Status.NOT_FOUND).header(HttpHeaders.CONTENT_TYPE, "text/plain").entity("No SPARQL query provided.").build

            // Execute the SPARQL query against the database
            val sparqlQuery = QueryFactory.create(query)
            MorphRunner.runner.runQuery(sparqlQuery)

            // Read the response from the output file and direct it to the HTTP response
            val file = new FileInputStream(MorphRunner.factory.getProperties.outputFilePath)
            return Response.status(Status.OK).
                header(headerAccept, "*").
                header(HttpHeaders.CONTENT_TYPE, "application/sparql-results+xml").
                entity(new InputStreamReader(file, "UTF-8")).build

            // Mime types:
            // XML : application/xml, text/xml
            // RDF: application/rdf+xml, text/turtle, text/nt
            // SPARQL result: application/sparql-results+xml, application/sparql-results+json, application/sparql-results+csv, application/sparql-results+tsv

        } catch {
            case e: Exception => {
                val msg = "Error in SPARQL query processing: " + e.getMessage
                logger.error(msg)
                if (logger.isDebugEnabled) logger.debug("Strack trace:\n" + e.getStackTraceString)
                return Response.status(Status.INTERNAL_SERVER_ERROR).header(HttpHeaders.CONTENT_TYPE, "text/plain").entity(msg).build
            }
        }
    }
}