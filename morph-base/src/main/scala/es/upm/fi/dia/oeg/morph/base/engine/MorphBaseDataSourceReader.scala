package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery

/**
 * A data source reader is used in case of the query rewriting access method,
 * to execute queries on the fly.
 * It is not used in the data materialization access method.
 */
abstract class MorphBaseDataSourceReader(val factory: IMorphFactory) {

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

    def setTimeout(timeout: Int)

    def closeConnection()
}