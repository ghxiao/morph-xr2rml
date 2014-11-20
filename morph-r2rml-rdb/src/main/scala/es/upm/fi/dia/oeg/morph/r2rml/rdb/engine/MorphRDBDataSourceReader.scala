package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

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
class MorphRDBDataSourceReader() extends MorphBaseDataSourceReader {
    var timeout: Int = 60;
    var connection: Connection = null;
    var dbType: String = null;

    override def execute(query: String): MorphBaseResultSet = {
        val rs = DBUtility.execute(this.connection, query, this.timeout);
        val abstractResultSet = new RDBResultSet(rs);
        abstractResultSet;
    }

    override def setConnection(connection: Object) = {
        this.connection = connection.asInstanceOf[Connection]
    }

    override def setTimeout(timeout: Int) = { this.timeout = timeout }

    override def closeConnection() = {
        DBUtility.closeConnection(this.connection, this.getClass().getName());
    }
}