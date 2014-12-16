package es.upm.fi.dia.oeg.morph.r2rml.model

import org.apache.log4j.Logger

import com.hp.hpl.jena.rdf.model.Resource

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData
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
    val docIterator: Option[String])

        extends MorphBaseLogicalTable with MorphR2RMLElement {

    val logger = Logger.getLogger(this.getClass().getName());
    var alias: String = null;

    def buildMetaData(dbMetaData: Option[MorphDatabaseMetaData]) = {

        val source = this match {
            case _: xR2RMLTable => this.asInstanceOf[xR2RMLTable]
            case _: xR2RMLQuery => this.asInstanceOf[xR2RMLQuery]
        }

        val tableName = this match {
            case xr2rmlTable: xR2RMLTable => {
                val dbType =
                    if (dbMetaData.isDefined) { dbMetaData.get.dbType; }
                    else { Constants.DATABASE_DEFAULT }
                val enclosedChar = Constants.getEnclosedCharacter(dbType);
                val tableNameAux = xr2rmlTable.getValue().replaceAll("\"", enclosedChar);
                tableNameAux
            }
            case xr2rmlQuery: xR2RMLQuery => {
                val queryStringAux = xr2rmlQuery.getValue().trim()
                val queryString = {
                    // Remove trailing ';' if any
                    if (queryStringAux.endsWith(";")) { queryStringAux.substring(0, queryStringAux.length() - 1) }
                    else { queryStringAux }
                }
                "(" + queryString + ")";
            }
            case _ => {
                val msg = "Unknown logical table/source type: " + this
                logger.error(msg)
                throw new Exception(msg)
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
            case _: xR2RMLTable => { "xR2RMLTable"; }
            case _: xR2RMLQuery => { "xR2RMLQuery"; }
            case _ => throw new Exception("Unkown type of logical source or logical table")
        }
        result + ": " + this.getValue() + ". ReferenceFormulation: " + this.refFormulation + ". Iterator: " + this.docIterator
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
     * @param reource an xrr:LogicalSource or rr:LogicalTable resource
     * @param logResType class URI of a logical source or logical table
     * @param refFormulation the reference formulation URI
     */
    def parse(resource: Resource, logResType: String, refFormulation: String): xR2RMLLogicalSource = {
        val logSrc: xR2RMLLogicalSource =
            {
                val tableNameStmt = resource.getProperty(Constants.R2RML_TABLENAME_PROPERTY)
                val sqlQueryStmt = resource.getProperty(Constants.R2RML_SQLQUERY_PROPERTY)
                val queryStmt = resource.getProperty(Constants.xR2RML_QUERY_PROPERTY)

                val source: String =
                    if (tableNameStmt != null)
                        "Table name: " + tableNameStmt.getObject().toString()
                    else if (sqlQueryStmt != null)
                        "SQL query: " + sqlQueryStmt.getObject().toString().trim()
                    else if (queryStmt != null)
                        "Query: " + queryStmt.getObject().toString().trim()
                    else
                        "Undefined property tableName, sqlQuery or query."

                // Check compliance with R2RML
                if (isLogicalTable(logResType)) {
                    if (queryStmt != null) {
                        val msg = "Logical Table cannot have an xrr:query property. " + source;
                        logger.error(msg);
                        throw new Exception(msg);
                    }
                    if (hasReferenceFormulation(resource)) {
                        val msg = "Logical Table cannot have an xrr:referenceFormulation property. " + source;
                        logger.error(msg);
                        throw new Exception(msg);
                    }
                    if (hasIterator(resource)) {
                        val msg = "Logical Table cannot have an rml:iterator property. " + source;
                        logger.error(msg);
                        throw new Exception(msg);
                    }
                }

                if (tableNameStmt != null) {
                    val tableName = tableNameStmt.getObject().toString()

                    // Check validity of optional properties iterator and referenceFormulation
                    if (hasIterator(resource))
                        logger.warn("Ignoring iterator [" + readIterator(resource).get + "] with rr:tableName " + tableName)
                    if (!isDefaultReferenceFormulation(resource)) {
                        val msg = "Invalid reference formulation: " + readReferenceFormulation(resource) + " with rr:tableName " + tableName
                        logger.error(msg);
                        throw new Exception(msg);
                    }

                    new xR2RMLTable(tableName)

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
                    if (hasIterator(resource))
                        logger.warn("Ignoring iterator \"" + readIterator(resource).get + "\" with rr:sqlQuery \"" + queryStr + "\"")
                    if (!isDefaultReferenceFormulation(resource)) {
                        val msg = "Invalid reference formulation: " + readReferenceFormulation(resource) + "\". Cannot be used with rr:sqlQuery \"" + queryStr + "\""
                        logger.error(msg);
                        throw new Exception(msg);
                    }

                    new xR2RMLQuery(queryStr, readReferenceFormulation(resource), readIterator(resource))

                } else if (queryStmt != null) {
                    // xR2RML query
                    val queryStr = queryStmt.getObject().toString().trim();
                    new xR2RMLQuery(queryStr, refFormulation, readIterator(resource))

                } else {
                    val errorMessage = "Missing logical source property: rr:tableName, rr:sqlQuery or xrr:query";
                    logger.error(errorMessage);
                    throw new Exception(errorMessage);
                }
            }
        logSrc;
    }

    /**
     * Return true is the resource has an xrr:referenceFormulation property
     */
    private def hasReferenceFormulation(resource: Resource): Boolean = {
        (resource.getProperty(Constants.xR2RML_REFFORMULATION_PROPERTY) != null)
    }

    /**
     * Return true is the resource has an rml:iterator property
     */
    private def hasIterator(resource: Resource): Boolean = {
        (resource.getProperty(Constants.RML_ITERATOR_PROPERTY) != null)
    }

    /**
     *  Read the rml:iterator property, return None if undefined
     */
    private def readIterator(resource: Resource): Option[String] = {
        val iterStmt = resource.getProperty(Constants.RML_ITERATOR_PROPERTY)
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
        var result = Constants.xR2RML_COLUMN_URI

        val refFormStmt = resource.getProperty(Constants.xR2RML_REFFORMULATION_PROPERTY)
        if (refFormStmt != null) {
            val refFormStr = refFormStmt.getObject().toString();
            if (!refFormStr.isEmpty())
                // Verify that the formulation is part of the list of authorized reference formulations
                if (Constants.xR2RML_AUTHZD_REFERENCE_FORMULATION.contains(refFormStr))
                    result = refFormStr
                else
                    logger.warn("Unknown reference formulation: " + refFormStr)
        }
        result
    }

    /** Return true is the reference formulation has the default value i.e. xrr:Column */
    private def isDefaultReferenceFormulation(resource: Resource): Boolean = {
        Constants.xR2RML_COLUMN_URI.equals(readReferenceFormulation(resource))
    }

    private def isLogicalSource(logResType: String): Boolean = {
        logResType.equals(Constants.xR2RML_LOGICALSOURCE_URI)
    }

    private def isLogicalTable(logResType: String): Boolean = {
        logResType.equals(Constants.R2RML_LOGICALTABLE_URI)
    }
}
