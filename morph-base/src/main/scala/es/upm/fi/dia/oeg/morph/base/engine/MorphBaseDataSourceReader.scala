package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericConnection

/**
 * A data source reader is used in case of the query rewriting access method,
 * to execute queries on the fly.
 * It is not used in the data materialization access method.
 */
class MorphBaseDataSourceReader() {

    var timeout: Int = 60
    var connection: GenericConnection = null
    var dbType: String = null;

    def execute(query: String): MorphBaseResultSet = {
        throw new Exception("Operation not supported.")
    }

    def setConnection(connection: GenericConnection) {
        throw new Exception("Operation not supported.")
    }

    def setTimeout(timeout: Int) {
        throw new Exception("Operation not supported.")
    }

    def closeConnection() {
        throw new Exception("Operation not supported.")
    }
}