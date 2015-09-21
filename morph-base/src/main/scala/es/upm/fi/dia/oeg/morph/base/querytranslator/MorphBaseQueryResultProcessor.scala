package es.upm.fi.dia.oeg.morph.base.querytranslator

import java.io.Writer

import com.hp.hpl.jena.query.Query

import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.UnionOfGenericQueries
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

abstract class MorphBaseQueryResultProcessor(
        var mappingDocument: R2RMLMappingDocument,
        var properties: MorphProperties,
        var outputStream: Writer,
        var dataSourceReader: MorphBaseDataSourceReader) {

    def preProcess(sparqlQuery: Query): Unit;

    def process(sparqlQuery: Query, resultSet: MorphBaseResultSet): Unit;

    def postProcess(): Unit;

    def getOutput(): Object;

    /**
     * Execute the query and translate the results from the database into triples.<br>
     * In the RDB case the UnionOfGenericQueries should contain only one element, since
     * the UNION is supported in SQL.<br>
     * Conversely, since MongoDB does not support UNIONs with WHEREs, there may be
     * several elements in UnionOfGenericQueries. In this case, the union should be done
     * "manually" by the xR2RML processing engine.
     */
    def translateResult(mapSparqlSql: Map[Query, UnionOfGenericQueries])

}
