package es.upm.fi.dia.oeg.morph.base.querytranslator

import java.io.Writer
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.query.AbstractQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.IMorphFactory
import java.io.File

/**
 * Abstract class for the engine that shall execute a query and translate results into
 * RDF triples (DESCRIBE, CONTRUCT) or a SPARQL result set (SELECT, ASK)
 */
abstract class MorphBaseQueryProcessor(factory: IMorphFactory) {

    /**
     * Execute the query, translate the results from the database into triples
     * or result sets, and serialize the result into an output file
     * 
     * @param sparqlQuery SPARQL query 
     * @param abstractQuery associated AbstractQuery resulting from the translation of sparqlQuery,
     * in which the executable target queries have been computed
     * @param syntax the output syntax:  XML or JSON for a SPARQL SELECT or ASK query, and RDF 
     * syntax for a SPARQL DESCRIBE or CONSTRUCT query
     */
    def process(sparqlQuery: Query, abstractQuery: AbstractQuery, syntax: String): Option[File]
}
