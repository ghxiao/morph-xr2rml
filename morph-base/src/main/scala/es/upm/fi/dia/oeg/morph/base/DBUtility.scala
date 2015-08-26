package es.upm.fi.dia.oeg.morph.base

import org.apache.log4j.Logger
import java.sql.Connection
import java.sql.SQLException
import java.sql.ResultSet
import java.sql.Statement
import java.util.Properties
import java.sql.DriverManager

class DBUtility {
}

object DBUtility {
    def logger = Logger.getLogger(this.getClass());

    def execute(conn: Connection, query: String, timeout: Integer): ResultSet = {
        logger.info("Executing query: " + query.replaceAll("\n", " "));

        if (conn == null) {
            val errorMessage = "No connection defined!";
            logger.error(errorMessage);
            throw new Exception(errorMessage);
        }

        val stmt = conn.createStatement(
            java.sql.ResultSet.TYPE_FORWARD_ONLY,
            java.sql.ResultSet.CONCUR_READ_ONLY);

        val dbmd = conn.getMetaData();
        val dbProductName = dbmd.getDatabaseProductName();
        if (Constants.DATABASE_MYSQL.equalsIgnoreCase(dbProductName)) {
            stmt.setFetchSize(Integer.MIN_VALUE);
        } else { stmt.setFetchSize(100); }

        if (timeout > 0) { stmt.setQueryTimeout(timeout); }
        val start = System.currentTimeMillis();
        val result = stmt.execute(query);
        val end = System.currentTimeMillis();
        logger.info("SQL execution time was " + (end - start) + " ms.");

        if (result) { stmt.getResultSet(); }
        else { null }

    }

    def closeConnection(conn: Connection, requesterString: String) = {
        try {
            if (conn != null) {
                conn.close();
                logger.info("Closing db connection.");
            }
        } catch {
            case e: Exception => {
                logger.error("Error closing connection! Error message = " + e.getMessage());
            }
        }
    }

    def closeRecordSet(rs: ResultSet) = {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch {
            case e: Exception => {
                logger.error("Error closing result set! Error message = " + e.getMessage());
            }
        }
    }

    def closeStatement(stmt: Statement) = {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch {
            case e: Exception => {
                logger.error("Error closing statement! Error message = " + e.getMessage());
            }
        }
    }

    def getRowCount(set: ResultSet): Integer = {
        val currentRow = set.getRow(); // Get current row

        val rowCount = {
            if (set.last()) {
                set.getRow()
            } else {
                0
            }
        }

        if (currentRow == 0) // If there was no current row  
            set.beforeFirst(); // We want next() to go to first row  
        else // If there WAS a current row  
            set.absolute(currentRow); // Restore it  
        return rowCount;
    }

    def getLocalConnection(username: String, databaseName: String, password: String, driverString: String, url: String, requester: String): Connection = {

        try {
            val prop = new Properties();
            prop.put("ResultSetMetaDataOptions", "1");
            prop.put("user", username);
            prop.put("database", databaseName);
            prop.put("password", password);
            prop.put("autoReconnect", "true");
            Class.forName(driverString);
            logger.info("Opening database connection.");
            val conn = DriverManager.getConnection(url, prop);
            conn.setAutoCommit(false);
            conn;
        } catch {
            case e: ClassNotFoundException => {
                val errorMessage = "Error opening database connection, class not found : " + e.getMessage();
                logger.error(errorMessage);
                null
            }
            case e: Exception => {
                logger.error("Error opening database connection : " + e.getMessage());
                null
            }
        }
    }

    /**
     * Simple display of a result set.
     * Warning: this consumes the result set, meaning that it can't be parsed anymore afterwards.
     * Use for debug only.
     */
    def resultSetToString(rs: ResultSet): String = {
        val rsmd = rs.getMetaData()
        val sep = sys.props("line.separator")
        val columnsNumber = rsmd.getColumnCount()
        var result: StringBuilder = new StringBuilder(sep)

        for (i <- 1 to columnsNumber) {
            if (i > 1) { result.append(" | ") }
            result.append(rsmd.getColumnName(i))
        }
        result.append(sep)

        while (rs.next()) {
            for (i <- 1 to columnsNumber) {
                if (i > 1) { result.append(" | ") }
                result.append(rs.getString(i))
            }
            result.append(sep)
        }
        result.toString
    }

    /**
     * Print out the content of the current row of a result set.
     */
    def resultSetCurrentRowToString(rs: ResultSet): String = {
        val rsmd = rs.getMetaData()
        val columnsNumber = rsmd.getColumnCount()
        var result: StringBuilder = new StringBuilder()

        for (i <- 1 to columnsNumber) {
            if (i > 1) { result.append(" | ") }
            result.append(rs.getString(i))
        }
        result.toString
    }

}