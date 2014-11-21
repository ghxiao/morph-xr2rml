package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.sql.Connection

import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.GenericConnection
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
    var sqlCnx: Connection = null;
    var dbType: String = null;

    override def execute(query: String): MorphBaseResultSet = {
        val rs = DBUtility.execute(this.sqlCnx, query, this.timeout);
        val abstractResultSet = new RDBResultSet(rs);
        abstractResultSet;
    }

    override def setConnection(connection: GenericConnection) = {       
        if (!connection.isRelationalDB)
            throw new Exception("Connection type is not relational database")
        this.sqlCnx = connection.concreteCnx.asInstanceOf[Connection]
    }

    override def setTimeout(timeout: Int) = { this.timeout = timeout }

    override def closeConnection() = {
        DBUtility.closeConnection(this.sqlCnx, this.getClass().getName());
    }
}