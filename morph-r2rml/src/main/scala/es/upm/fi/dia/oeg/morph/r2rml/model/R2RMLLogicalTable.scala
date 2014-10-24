package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.model.MorphBaseLogicalTable
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

/**
 * Abstract class to represent an LogicalTable, is inherited by R2RMLTable and R2RMLSqlQuey
 */
abstract class R2RMLLogicalTable(val logicalTableType: Constants.LogicalTableType.Value, format: Option[String])
        extends MorphBaseLogicalTable with MorphR2RMLElement {

    val logger = Logger.getLogger(this.getClass().getName());
    var alias: String = null;

    def buildMetaData(dbMetaData: Option[MorphDatabaseMetaData]) = {
        val tableName = this match {
            case r2rmlTable: R2RMLTable => {
                val dbType = if (dbMetaData.isDefined) { dbMetaData.get.dbType; }
                else { Constants.DATABASE_DEFAULT }

                val enclosedChar = Constants.getEnclosedCharacter(dbType);
                val tableNameAux = r2rmlTable.getValue().replaceAll("\"", enclosedChar);
                tableNameAux
            }
            case r2rmlSQLQuery: R2RMLSQLQuery => {
                val queryStringAux = r2rmlSQLQuery.getValue().trim();
                val queryString = {
                    if (queryStringAux.endsWith(";")) {
                        queryStringAux.substring(0, queryStringAux.length() - 1);
                    } else { queryStringAux }
                }
                "(" + queryString + ")";
            }
        }

        if (dbMetaData.isDefined) {
            val optionTableMetaData = dbMetaData.get.getTableMetaData(tableName);
            val tableMetaData = optionTableMetaData.getOrElse(
                MorphTableMetaData.buildTableMetaData(tableName, dbMetaData.get));

            this.tableMetaData = Some(tableMetaData);
        }

    }

    /** @note xR2RML */
    def getFormat(): String = {
        format.getOrElse(xR2RML_Constants.xR2RML_RRXROW_URI)
    }

    /** Return the table name or query depending on the type of logical table */
    def getValue(): String;

    override def toString(): String = {
        val result = this match {
            case _: R2RMLTable => { "R2RMLTable"; }
            case _: R2RMLSQLQuery => { "R2RMLSQLQuery"; }
            case _ => { "" }
        }

        result + ":" + this.getValue();
    }

    def accept(visitor: MorphR2RMLElementVisitor): Object = {
        val result = visitor.visit(this);
        result;
    }
}

object R2RMLLogicalTable {
    val logger = Logger.getLogger(this.getClass().getName());

    def parse(resource: Resource): R2RMLLogicalTable = {

        /** @note xR2RML xrr:format */
        val formatRes = resource.getProperty(xR2RML_Constants.xR2RML_FORMAT_PROPERTY)
        // If there is no xrr:format, then default format xrr:Row applies.
        val formatStr = if (formatRes != null) {
            formatRes.getObject().toString();
        } else {
            xR2RML_Constants.xR2RML_RRXROW_URI
        }

        val tableNameStmt = resource.getProperty(Constants.R2RML_TABLENAME_PROPERTY);
        val logicalTable: R2RMLLogicalTable = if (tableNameStmt != null) {
            new R2RMLTable(tableNameStmt.getObject().toString(), Some(formatStr));
        } else {
            val sqlQueryStatement = resource.getProperty(Constants.R2RML_SQLQUERY_PROPERTY);
            if (sqlQueryStatement == null) {
                var msg = "Error: logical table defined with no ssqlQuery nor tableName: " + resource
                logger.error(msg);
                throw new Exception(msg)
            }
            val sqlQueryStringAux = sqlQueryStatement.getObject().toString().trim();
            val sqlQueryString = if (sqlQueryStringAux.endsWith(";")) {
                // remove tailing ';'
                sqlQueryStringAux.substring(0, sqlQueryStringAux.length() - 1);
            } else {
                sqlQueryStringAux
            }

            new R2RMLSQLQuery(sqlQueryString, Some(formatStr));
        }

        logicalTable;
    }
}