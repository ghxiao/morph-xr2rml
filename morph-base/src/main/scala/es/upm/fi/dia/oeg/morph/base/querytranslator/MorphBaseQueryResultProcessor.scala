package es.upm.fi.dia.oeg.morph.base.querytranslator

import java.io.Writer
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.AbstractQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator

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
     * Execute the query and translate the results from the database into triples.
     *
     * Execute the query and translate the results from the database into triples.<br>
     * In the RDB case the AbstractQuery should contain only one element, since
     * the UNION is supported in SQL.<br>
     *
     * Conversely, MongoDB does not support joins, therefore there may be several queries in the AbstractQuery.
     * In this case the xR2RML processor shall compute the join itself.
     */
    def translateResult(mapSparqlSql: Map[Query, AbstractQuery])

}
