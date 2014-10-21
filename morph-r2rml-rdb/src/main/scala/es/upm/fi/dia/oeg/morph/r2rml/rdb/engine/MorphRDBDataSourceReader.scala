package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine
import java.sql.ResultSet
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.engine.RDBResultSet
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader

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