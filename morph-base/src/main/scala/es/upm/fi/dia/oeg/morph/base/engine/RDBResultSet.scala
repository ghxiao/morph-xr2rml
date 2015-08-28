package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.ResultSet

/**
 * A helper class that encapsulates an SQL result set
 */
class RDBResultSet(rs: ResultSet) extends MorphBaseResultSet {
    override def next(): Boolean = {
        try { this.rs.next(); }
        catch {
            case e: Exception => { false; }
        }
    }

    override def getObject(columnIndex: Int): Object = {
        rs.getObject(columnIndex);
    }

    override def getObject(columnLabel: String): Object = {
        rs.getObject(columnLabel);
    }

    override def getString(columnIndex: Int): String = {
        rs.getString(columnIndex);
    }

    override def getString(columnLabel: String): String = {
        rs.getString(columnLabel);
    }

    override def getInt(columnIndex: Int): Integer = {
        return rs.getInt(columnIndex);
    }

    override def getInt(columnLabel: String): Integer = {
        rs.getInt(columnLabel);
    }
}