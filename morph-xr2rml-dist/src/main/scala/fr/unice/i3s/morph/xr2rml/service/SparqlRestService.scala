package fr.unice.i3s.morph.xr2rml.service

import org.apache.log4j.Logger

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status
import javax.ws.rs.core.HttpHeaders
import javax.ws.rs.HeaderParam

@Path("/sparql")
class SparqlrestService {

    val headerAccept = "Access-Control-Allow-Origin"

    val logger = Logger.getLogger(this.getClass)

    @GET
    @Path("test")
    @Produces(Array("text/plain"))
    def test: Response = {
        return Response.status(Status.OK).header(headerAccept, "*").entity("SPARQL REST service is up and running").build
    }

    @GET
    @Produces(Array("application/sparql-results+xml"))
    def getTriplesXMLForGet(@QueryParam("query") query: String,
                            @QueryParam("default-graph-uri") defaultGraphUris: java.util.List[String],
                            @QueryParam("named-graph-uri") namedGraphUris: java.util.List[String]): Response = {

        if (logger.isDebugEnabled) {
            logger.debug("GET XML, SPARQL query: " + query)
            logger.debug("GET XML, default graph: " + defaultGraphUris)
            logger.debug("GET XML, named graph: " + namedGraphUris)
        }

        try {
            if (query == null)
                return Response.status(Status.NOT_FOUND).
                    header(headerAccept, "*").
                    entity("No SPARQL query provided.").build

            return Response.status(Status.OK).
                header(headerAccept, "*").
                entity("result").build

        } catch {
            case e: Exception => {
                val msg = "Error in SPARQL query processing: " + e.getMessage
                logger.error(msg)
                logger.error("\n" + e.getStackTraceString)
                return Response.status(Status.INTERNAL_SERVER_ERROR).
                    header(headerAccept, "*").
                    entity(msg).build
            }
        }
    }

}