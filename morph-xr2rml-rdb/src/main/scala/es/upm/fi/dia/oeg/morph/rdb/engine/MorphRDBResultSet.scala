package es.upm.fi.dia.oeg.morph.rdb.engine

import java.sql.ResultSet

import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet

/**
 * A helper class that encapsulates an SQL result set
 */
class MorphRDBResultSet(val resultSet: ResultSet) extends MorphBaseResultSet {
    var columnNames: List[String] = null;

    def next(): Boolean = {
        try { this.resultSet.next(); }
        catch {
            case e: Exception => { false; }
        }
    }

    def getObject(columnIndex: Int): Object = {
        resultSet.getObject(columnIndex);
    }

    def getObject(columnLabel: String): Object = {
        resultSet.getObject(columnLabel);
    }

    def getString(columnIndex: Int): String = {
        resultSet.getString(columnIndex);
    }

    def getString(columnLabel: String): String = {
        resultSet.getString(columnLabel);
    }

    def getInt(columnIndex: Int): Integer = {
        return resultSet.getInt(columnIndex);
    }

    def getInt(columnLabel: String): Integer = {
        resultSet.getInt(columnLabel);
    }

    def getColumnNames(): List[String] = {
        this.columnNames;
    }

    def setColumnNames(columnNames: List[String]) {
        this.columnNames = columnNames;
    }

    def getColumnName(columnIndex: Int) = {
        this.columnNames(columnIndex);
    }
}