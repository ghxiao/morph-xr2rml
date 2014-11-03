package es.upm.fi.dia.oeg.morph.r2rml.model

import org.apache.log4j.Logger

import com.hp.hpl.jena.rdf.model.Resource

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor

/**
 * Abstract class to represent an xR2RML LogicalSource, is inherited by R2RMLTable, R2RMLSqlQuey and xR2RMLQuery.
 *
 * This class is a refactoring of former R2RMLLogicalTable class.
 */
abstract class xR2RMLLogicalSource(
    val logicalTableType: Constants.LogicalTableType.Value,

    /** Syntax of data elements references (iterator, reference, template). Defaults to xrr:Column */
    val refFormulation: String,

    /** Iteration pattern, defaults to none */
    val iterator: Option[String])

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

    /** Return the table name or query depending on the type of logical table */
    def getValue(): String;

    override def toString(): String = {
        val result = this match {
            case _: R2RMLTable => { "R2RMLTable"; }
            case _: xR2RMLTable => { "xR2RMLTable"; }
            case _: R2RMLSQLQuery => { "R2RMLSQLQuery"; }
            case _: xR2RMLQuery => { "xR2RMLQuery"; }
            case _ => throw new Exception("Unkown type of logical source or logical table")
        }
        result + ": " + this.getValue() + ". ReferenceFormulation: " + this.refFormulation + ". Iterator: " + this.iterator
    }

    def accept(visitor: MorphR2RMLElementVisitor): Object = {
        val result = visitor.visit(this);
        result;
    }
}

object xR2RMLLogicalSource {
    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Check properties of a logical source or logical table to create the appropriate
     * instance of R2RMLTable, R2RMLSqlQuey and xR2RMLQuery.
     *
     * @param reource : an xrr:LogicalSource or rr:LogicalTable resource
     * @param logResType : string indicating whether this is a logical source or logical table
     */
    def parse(resource: Resource, logResType: String): xR2RMLLogicalSource = {

        val logSrc: xR2RMLLogicalSource =
            {
                val tableNameStmt = resource.getProperty(Constants.R2RML_TABLENAME_PROPERTY)
                val sqlQueryStmt = resource.getProperty(Constants.R2RML_SQLQUERY_PROPERTY)
                val queryStmt = resource.getProperty(xR2RML_Constants.xR2RML_QUERY_PROPERTY)

                if (tableNameStmt != null) {
                    val tableName = tableNameStmt.getObject().toString()

                    // Check validity of optional properties iterator and referenceFormulation
                    if (readIterator(resource).isDefined)
                        logger.warn("Ignoring iterator with rr:tableName " + tableName)
                    if (!isDefaultReferenceFormulation(resource))
                        logger.warn("Ignoring reference formulation: " + readReferenceFormulation(resource) + ". Cannot be used with rr:tableName " + tableName)

                    if (isLogicalSource(logResType))
                        new R2RMLTable(tableName)
                    else
                        new xR2RMLTable(tableName, readReferenceFormulation(resource))

                } else if (sqlQueryStmt != null) {

                    // Regular R2RML view
                    val queryStringAux = sqlQueryStmt.getObject().toString().trim();
                    val queryStr =
                        if (queryStringAux.endsWith(";")) {
                            queryStringAux.substring(0, queryStringAux.length() - 1); // remove tailing ';'
                        } else {
                            queryStringAux
                        }

                    // Check validity of optional properties iterator and referenceFormulation
                    if (isLogicalSource(logResType)) {
                        val errorMessage = "Property rr:sqlQuery is not allowed in a xrr:logicalSource";
                        logger.error(errorMessage);
                        throw new Exception(errorMessage);
                    }
                    if (readIterator(resource).isDefined)
                        logger.warn("Ignoring iterator \"" + readIterator(resource).get + "\" with rr:sqlQuery \"" + queryStr + "\"")
                    if (!isDefaultReferenceFormulation(resource))
                        logger.warn("Ignoring reference formulation \"" + readReferenceFormulation(resource) + "\". Cannot be used with rr:sqlQuery \"" + queryStr + "\"")

                    new R2RMLSQLQuery(queryStr)

                } else if (queryStmt != null) {
                    // xR2RML query
                    if (isLogicalTable(logResType)) {
                        val errorMessage = "Property xrr:query is not allowed in the context of a rr:logicalTable";
                        logger.error(errorMessage);
                        throw new Exception(errorMessage);
                    }
                    val queryStr = queryStmt.getObject().toString().trim();
                    new xR2RMLQuery(queryStr, readReferenceFormulation(resource), readIterator(resource))

                } else {
                    val errorMessage = "Missing logical source property: rr:tableName, rr:sqlQuery or xrr:query";
                    logger.error(errorMessage);
                    throw new Exception(errorMessage);
                }
            }
        logSrc;
    }

    /** Read the rml:iterator property, return None if undefined */
    private def readIterator(resource: Resource): Option[String] = {
        val iterStmt = resource.getProperty(xR2RML_Constants.RML_ITERATOR_PROPERTY)
        val iter: Option[String] =
            if (iterStmt == null)
                None
            else {
                val iterStr = iterStmt.getObject().toString().trim();
                if (!iterStr.isEmpty())
                    Some(iterStr)
                else None
            }
        iter
    }

    /**
     * Read the reference formulation property and checks it has is an authorized value.
     * Return xrr:Column as a default if property if not defined.
     */
    private def readReferenceFormulation(resource: Resource): String = {
        var result = xR2RML_Constants.xR2RML_COLUMN_URI

        val refFormStmt = resource.getProperty(xR2RML_Constants.xR2RML_REFFORMULATION_PROPERTY)
        if (refFormStmt != null) {
            val refFormStr = refFormStmt.getObject().toString();
            if (!refFormStr.isEmpty())
                // Verify that the formulation is part of the list of authorized reference formulations
                if (xR2RML_Constants.xR2RML_AUTHZD_REFERENCE_FORMULATION.contains(refFormStr))
                    result = refFormStr
                else
                    logger.warn("Unknown reference formulation: " + refFormStr)
        }
        result
    }

    /** Return true is the reference formulation has the default value i.e. xrr:Column */
    private def isDefaultReferenceFormulation(resource: Resource): Boolean = {
        xR2RML_Constants.xR2RML_COLUMN_URI.equals(readReferenceFormulation(resource))
    }

    private def isLogicalSource(logResType: String): Boolean = {
        logResType.equals(xR2RML_Constants.xR2RML_LOGICALSOURCE_URI)
    }

    private def isLogicalTable(logResType: String): Boolean = {
        logResType.equals(Constants.R2RML_LOGICALTABLE_URI)
    }
}
