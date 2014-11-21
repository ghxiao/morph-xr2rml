package fr.unice.i3s.morph.xr2rml.jsondoc.engine

import java.sql.Connection

import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.engine.RDBResultSet

/**
 * This class is used in case of the query rewriting access method,
 * to execute queries on the fly.
 * It is not used in the data materialization access method.
 */
class MorphJsondocDataSourceReader() extends MorphBaseDataSourceReader {

    var timeout: Int = 60;
    var connection: Connection = null;
    var dbType: String = null;

    override def execute(query: String): MorphBaseResultSet = {
        throw new Exception("Operation not supported.")
    }

    override def setConnection(connection: Object) = {
        throw new Exception("Operation not supported.")
    }

    override def setTimeout(timeout: Int) = {
        throw new Exception("Operation not supported.")
    }

    override def closeConnection() = {
        throw new Exception("Operation not supported.")
    }
}