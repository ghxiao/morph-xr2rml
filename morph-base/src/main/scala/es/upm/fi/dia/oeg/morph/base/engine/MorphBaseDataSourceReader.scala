package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.GenericQuery

/**
 * A data source reader is used in case of the query rewriting access method,
 * to execute queries on the fly.
 * It is not used in the data materialization access method.
 */
abstract class MorphBaseDataSourceReader() {

    var timeout: Int = 60
    var connection: GenericConnection = null
    var dbType: String = null;

    /**
     * Execute a target database query against the connection.
     * @return a concrete instance of MorphBaseResultSet. Must NOT return null, may return an empty result.
     */
    def execute(query: GenericQuery): MorphBaseResultSet

    def setConnection(connection: GenericConnection)

    def setTimeout(timeout: Int)

    def closeConnection()
}