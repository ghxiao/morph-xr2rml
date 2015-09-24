package es.upm.fi.dia.oeg.morph.base.querytranslator

import java.io.Writer

import com.hp.hpl.jena.query.Query

import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.UnionOfGenericQueries
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

/**
 * Abstract class for the engine that shall translate results of a SPARQL query into RDF triples
 */
abstract class MorphBaseQueryResultProcessor(
        var mappingDocument: R2RMLMappingDocument,
        var properties: MorphProperties,
        var outputStream: Writer) {

    def preProcess(sparqlQuery: Query): Unit;

    def process(sparqlQuery: Query, resultSet: MorphBaseResultSet): Unit;

    def postProcess(): Unit;

    def getOutput(): Object;

    /**
     * <p>Execute the query and translate the results from the database into triples.</p>
     *
     * <p>In the RDB case, the UnionOfGenericQueries is a bit exaggerated ;-): it should contain only
     * a child query (there is no need to split child and parent queries since SQL supports joins),
     * and exactly one element in the child query (since SQL supports the UNION).</p>
     *
     * <p>Conversely, MongoDB does not support the JOIN, therefore there may be a child <em>and</em> a parent query.
     * and since it does not support UNIONs with WHEREs, there may be several elements in each child of parent queries.
     * MongoDB does support UNIONs (by means of the $or operator), but only if there is no $where as members of the $or.
     * Therefore it is not always possible to create a MongoDB query that is equivalent to the SPARQL query. 
     * In this case, several concrete queries are returned, and the xR2RML processor shall compute the union itself.</p>
     */
    def translateResult(mapSparqlSql: Map[Query, UnionOfGenericQueries])

}
