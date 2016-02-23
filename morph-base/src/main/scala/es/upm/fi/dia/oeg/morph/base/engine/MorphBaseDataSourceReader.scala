package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

/**
 * A data source reader is used in case of the query rewriting access method,
 * to execute queries on the fly.
 * It is not used in the data materialization access method.
 */
abstract class MorphBaseDataSourceReader(
        val md: R2RMLMappingDocument,
        val properties: MorphProperties) {

    var timeout: Int = 60
    var connection: GenericConnection = null
    var dbType: String = null;

    /**
     * Execute a target database query against the connection.
     * @return a concrete instance of MorphBaseResultSet. Must NOT return null, may return an empty result.
     */
    def execute(query: GenericQuery): MorphBaseResultSet

    /**
     * Execute a target database against the connection and apply an rml:iterator on the results.
     * @return a concrete instance of MorphBaseResultSet. Must NOT return null, may return an empty result.
     */
    def executeQueryAndIterator(query: GenericQuery, logSrcIterator: Option[String]): MorphBaseResultSet

    def setConnection(connection: GenericConnection)

    def setTimeout(timeout: Int)

    def closeConnection()
}