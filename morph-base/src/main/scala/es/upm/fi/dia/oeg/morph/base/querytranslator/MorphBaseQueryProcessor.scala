package es.upm.fi.dia.oeg.morph.base.querytranslator

import java.io.Writer
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.query.AbstractQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.IMorphFactory

/**
 * Abstract class for the engine that shall execute a query and translate results into
 * RDF triples (DESCRIBE, CONTRUCT) or a SPARQL result set (SELECT, ASK)
 */
abstract class MorphBaseQueryProcessor(factory: IMorphFactory) {

    /**
     * Execute the query, translate the results from the database into triples
     * or result sets, and serialize the result into an output file
     */
    def process(sparqlQuery: Query, abstractQuery: AbstractQuery)
}
