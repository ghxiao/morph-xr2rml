package es.upm.fi.dia.oeg.morph.rdb.engine

import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.base.sql.ISqlQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

/**
 * This class is used in case of the query rewriting access method,
 * to execute queries on the fly.
 * It is not used in the data materialization access method.
 */
class MorphRDBDataSourceReader(md: R2RMLMappingDocument, properties: MorphProperties)
        extends MorphBaseDataSourceReader(md, properties) {

    var sqlCnx: Connection = null;

    override def execute(query: GenericQuery): MorphBaseResultSet = {
        val rs = DBUtility.execute(this.sqlCnx, query.concreteQuery.asInstanceOf[ISqlQuery].toString(), this.timeout);
        val resultSet = new MorphRDBResultSet(rs);
        resultSet;
    }

    override def executeQueryAndIterator(query: GenericQuery, logSrcIterator: Option[String]): MorphBaseResultSet = {
        throw new MorphException("Unsupported method.")
    }

    override def setConnection(connection: GenericConnection) {
        if (!connection.isRelationalDB)
            throw new MorphException("Connection type is not relational database")
        this.connection = connection
        this.sqlCnx = connection.concreteCnx.asInstanceOf[Connection]
    }

    override def setTimeout(timeout: Int) {
        this.timeout = timeout
    }

    override def closeConnection() {
        DBUtility.closeConnection(this.sqlCnx, this.getClass().getName());
    }
}