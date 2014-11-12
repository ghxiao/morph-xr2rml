package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.util.Collection

import scala.collection.JavaConversions._

import org.apache.log4j.Logger

import Zql.ZConstant
import Zql.ZExpression
import Zql.ZQuery
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.SQLFromItem
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLJoinCondition
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLTable

class MorphRDBUnfolder(md: R2RMLMappingDocument, properties: MorphProperties)
        extends MorphBaseUnfolder(md, properties) with MorphR2RMLElementVisitor {

    val logger = Logger.getLogger(this.getClass().getName());

    /** List the SQL aliases of all columns referenced by each term map of the triples map */
    var mapTermMapColumnsAliases: Map[Object, List[String]] = Map.empty;

    /** List the SQL aliases of referencing object maps of the triples map */
    var mapRefObjectMapAlias: Map[R2RMLRefObjectMap, String] = Map.empty;

    def getAliases(termMapOrRefObjectMap: Object): Collection[String] = {
        if (this.mapTermMapColumnsAliases.get(termMapOrRefObjectMap).isDefined) {
            this.mapTermMapColumnsAliases(termMapOrRefObjectMap);
        } else {
            null
        }
    }

    def getMapRefObjectMapAlias(): Map[R2RMLRefObjectMap, String] = {
        return mapRefObjectMapAlias;
    }

    /**
     * Build an instance of SQLLogicalTable, that may be either an
     * SQLFromItem in case of a logical table with a table name,
     * or an SQLQuery in case of a logical table with a query string
     *
     * @return instance of SQLFromItem or SQLQuery
     */
    def unfoldLogicalSource(logicalTable: xR2RMLLogicalSource): SQLLogicalTable = {
        val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);

        val result = logicalTable.logicalTableType match {

            case Constants.LogicalTableType.TABLE_NAME => {
                val logTableValWithEnclosedChar = logicalTable.getValue().replaceAll("\"", dbEnclosedCharacter);
                val resultAux = new SQLFromItem(logTableValWithEnclosedChar, Constants.LogicalTableType.TABLE_NAME);
                resultAux.databaseType = this.dbType;
                resultAux
            }
            case Constants.LogicalTableType.QUERY => {
                val sqlString = logicalTable.getValue().replaceAll("\"", dbEnclosedCharacter);
                // Add tailing ';' if not already there
                val sqlString2 =
                    if (!sqlString.endsWith(";"))
                        sqlString + ";"
                    else sqlString

                try { MorphRDBUtility.toSQLQuery(sqlString2) }
                catch {
                    case e: Exception => {
                        logger.warn("Not able to parse the query, string will be used.");
                        val resultAux = new SQLFromItem(sqlString, Constants.LogicalTableType.QUERY);
                        resultAux.databaseType = this.dbType;
                        resultAux
                    }
                }
            }
            case _ => { throw new Exception("Invalid logical table type" + logicalTable.logicalTableType) }
        }
        result;
    }

    /**
     * Return a list of select items corresponding to the columns referenced by the term map.
     * Nil in case of a constant-valued term-map, a list of one select item for a column-valued term map,
     * and a list of several select items for a template-valued term map.
     */
    def unfoldTermMap(termMap: R2RMLTermMap, logicalTableAlias: String): List[MorphSQLSelectItem] = {

        val result =
            if (termMap != null) {
                termMap.termMapType match {

                    case Constants.MorphTermMapType.ConstantTermMap => { Nil }

                    case Constants.MorphTermMapType.ColumnTermMap => {
                        val selectItem = MorphSQLSelectItem.apply(termMap.columnName, logicalTableAlias, dbType);
                        if (selectItem != null) {
                            if (selectItem.getAlias() == null) {
                                val alias = selectItem.getTable() + "_" + selectItem.getColumn();
                                selectItem.setAlias(alias);
                                if (this.mapTermMapColumnsAliases.containsKey(termMap)) {
                                    val oldColumnAliases = this.mapTermMapColumnsAliases(termMap);
                                    val newColumnAliases = oldColumnAliases ::: List(alias);
                                    this.mapTermMapColumnsAliases += (termMap -> newColumnAliases);
                                } else {
                                    this.mapTermMapColumnsAliases += (termMap -> List(alias));
                                }
                            }
                        }
                        List(selectItem)
                    }

                    case Constants.MorphTermMapType.ReferenceTermMap => {
                        val columns = termMap.getReferencedColumns();
                        if (columns.isEmpty) { Nil }
                        else {
                            columns.map(column => {
                                val selectItem = MorphSQLSelectItem.apply(column, logicalTableAlias, dbType);
                                if (selectItem != null) {
                                    if (selectItem.getAlias() == null) {
                                        val alias = selectItem.getTable() + "_" + selectItem.getColumn();
                                        selectItem.setAlias(alias);
                                        if (this.mapTermMapColumnsAliases.containsKey(termMap)) {
                                            val oldColumnAliases = this.mapTermMapColumnsAliases(termMap);
                                            val newColumnAliases = oldColumnAliases ::: List(alias);
                                            this.mapTermMapColumnsAliases += (termMap -> newColumnAliases);
                                        } else
                                            this.mapTermMapColumnsAliases += (termMap -> List(alias));
                                    }
                                }
                                selectItem
                            })
                        };
                    }

                    case Constants.MorphTermMapType.TemplateTermMap => {
                        val columns = termMap.getReferencedColumns();
                    	logger.trace("Columns referenced in the template: " + columns)
                        if (columns.isEmpty) { Nil }
                        else {
                            columns.map(column => {
                                val selectItem = MorphSQLSelectItem.apply(column, logicalTableAlias, dbType);
                                if (selectItem != null) {
                                    if (selectItem.getAlias() == null) {
                                        val alias = selectItem.getTable() + "_" + selectItem.getColumn();
                                        selectItem.setAlias(alias);
                                        if (this.mapTermMapColumnsAliases.containsKey(termMap)) {
                                            val oldColumnAliases = this.mapTermMapColumnsAliases(termMap);
                                            val newColumnAliases = oldColumnAliases ::: List(alias);
                                            this.mapTermMapColumnsAliases += (termMap -> newColumnAliases);
                                        } else
                                            this.mapTermMapColumnsAliases += (termMap -> List(alias));
                                    }
                                }
                                selectItem
                            })
                        };
                    }

                    case _ => { throw new Exception("Invalid term map type") }
                }
            } else { Nil }

        result
    }

    /**
     * Unfolding a triples map means to progressively build an SQL query by accumulating pieces:
     * (1) create the FROM clause from the logical table,
     * (2) for each column in the subject predicate and object maps, add items to the SELECT clause,
     * (3) for each column in the parent triples map of each referencing object map, add items of the SELECT clause,
     * (4) for each join condition, add an SQL WHERE condition and an alias in the FROM clause for the parent table,
     * (5) xR2RML: for each column of each join condition, add items to the SELECT clause.
     *
     * @return an SQLQuery (IQuery) describing the actual SQL query to be run against the RDB
     */
    def unfoldTriplesMap(
        triplesMapId: String,
        logicalSrc: xR2RMLLogicalSource,
        subjectMap: R2RMLSubjectMap,
        poms: Collection[R2RMLPredicateObjectMap]): IQuery = {

        val result = new SQLQuery();
        result.setDatabaseType(this.dbType);

        // UNFOLD LOGICAL SOURCE: build an SQL from item with all tables in the logical source
        val logicalSrcUnfolded: SQLFromItem = logicalSrc match {
            case _: xR2RMLTable => {
                this.unfoldLogicalSource(logicalSrc).asInstanceOf[SQLFromItem];
            }
            case _: xR2RMLQuery => {
                val logicalTableAux = this.unfoldLogicalSource(logicalSrc)
                logicalTableAux match {
                    case _: SQLQuery => {
                        val zQuery = this.unfoldLogicalSource(logicalSrc).asInstanceOf[ZQuery];
                        val resultAux = new SQLFromItem(zQuery.toString(), Constants.LogicalTableType.QUERY);
                        resultAux.databaseType = this.dbType
                        resultAux
                    }
                    case sqlFromItem: SQLFromItem => { sqlFromItem; }
                    case _ => { null }
                }
            }
            case _ => { throw new Exception("Unknown logical table/source type: " + logicalSrc) }
        }

        // Create an alias for the sub-query in the FROM clause
        val logicalTableAlias = logicalSrcUnfolded.generateAlias()
        logicalSrc.alias = logicalTableAlias
        val logicalTableUnfoldedJoinTable = new SQLJoinTable(logicalSrcUnfolded)
        result.addFromItem(logicalTableUnfoldedJoinTable)
        logger.trace("Unfolded logical source: " + result.toString.replaceAll("\n", ""))

        // Unfold subject map
        val subjectMapSelectItems = this.unfoldTermMap(subjectMap, logicalTableAlias);
        result.addSelectItems(subjectMapSelectItems);
        logger.trace("Unfolded subject map: " + result.toString.replaceAll("\n", ""))

        // Unfold predicate-object maps
        if (poms != null) {
            for (pom <- poms) {
                // Unfold all predicateMaps of the current predicate-object map 
                val predicateMaps = pom.predicateMaps;
                if (predicateMaps != null && !predicateMaps.isEmpty()) {
                    for (pm <- pom.predicateMaps) {
                        val selectItems = this.unfoldTermMap(pm, logicalTableAlias);
                        result.addSelectItems(selectItems);
                    }
                }

                // Unfold all objectMaps of the current predicate-object map 
                val objectMaps = pom.objectMaps;
                if (objectMaps != null && !objectMaps.isEmpty()) {
                    for (om <- pom.objectMaps) {
                        val selectItems = this.unfoldTermMap(om, logicalTableAlias);
                        result.addSelectItems(selectItems);
                    }
                }

                // Unfold RefObjectMaps
                val refObjectMaps = pom.refObjectMaps;
                if (refObjectMaps != null && !refObjectMaps.isEmpty()) {
                    // ############ @TODO Limitation here: only the first RefObjectMap is considered
                    val refObjectMap = pom.getRefObjectMap(0);
                    if (refObjectMap != null) {
                        val parentTriplesMap = this.md.getParentTriplesMap(refObjectMap);
                        val parentLogicalTable = parentTriplesMap.logicalSource
                        if (parentLogicalTable == null) {
                            val errorMessage = "Parent logical table is not found for RefObjectMap : " + pom.getMappedPredicateName(0);
                            throw new Exception(errorMessage);
                        }
                        val sqlParentLogicalTable = this.unfoldLogicalSource(parentLogicalTable.asInstanceOf[xR2RMLLogicalSource]);
                        val joinQueryAlias = sqlParentLogicalTable.generateAlias();

                        sqlParentLogicalTable.setAlias(joinQueryAlias);
                        this.mapRefObjectMapAlias += (refObjectMap -> joinQueryAlias);
                        pom.setAlias(joinQueryAlias);

                        val parentSubjectMap = parentTriplesMap.subjectMap;
                        // Get names of the columns referenced in the parent triples map
                        val refObjectMapColumnsString = parentSubjectMap.getReferencedColumns;
                        if (refObjectMapColumnsString != null) {
                            for (refObjectMapColumnString <- refObjectMapColumnsString) {
                                val selectItem = MorphSQLSelectItem(refObjectMapColumnString, joinQueryAlias, dbType, null);
                                if (selectItem.getAlias() == null) {
                                    val alias = selectItem.getTable() + "_" + selectItem.getColumn();
                                    selectItem.setAlias(alias);
                                    if (this.mapTermMapColumnsAliases.containsKey(refObjectMap)) {
                                        val oldColumnAliases = this.mapTermMapColumnsAliases(refObjectMap);
                                        val newColumnAliases = oldColumnAliases ::: List(alias);
                                        this.mapTermMapColumnsAliases += (refObjectMap -> newColumnAliases);
                                    } else {
                                        this.mapTermMapColumnsAliases += (refObjectMap -> List(alias));
                                    }
                                }
                                result.addSelectItem(selectItem);
                            }
                        }

                        val joinConditions = refObjectMap.getJoinConditions();
                        val onExpression = MorphRDBUnfolder.unfoldJoinConditions(joinConditions, logicalTableAlias, joinQueryAlias, dbType);
                        val joinQuery = new SQLJoinTable(sqlParentLogicalTable, Constants.JOINS_TYPE_LEFT, onExpression);
                        result.addFromItem(joinQuery);

                        /**
                         * @note XR2RML:
                         *  The joined columns are not necessarily referenced in the term maps.
                         *  However in the case of xR2RML, if the joined columns do not contain simple values but
                         *  structured values (e.g. an XML value that must be evaluated by an XPath expression), then
                         *  the join operation cannot be done by the database itself (in the SQL query) but afterwards in
                         *  the code. Therefore we add the joined columns in the select clause to have those values in case
                         *  we have to make the join afterwards
                         */
                        for (join <- joinConditions) {
                            var selectItem = MorphSQLSelectItem(join.childColumnName, logicalTableAlias, dbType, null);
                            if (selectItem.getAlias() == null) {
                                val alias = selectItem.getTable() + "_" + selectItem.getColumn();
                                selectItem.setAlias(alias);
                                if (this.mapTermMapColumnsAliases.containsKey(refObjectMap)) {
                                    val oldColumnAliases = this.mapTermMapColumnsAliases(refObjectMap);
                                    val newColumnAliases = oldColumnAliases ::: List(alias);
                                    this.mapTermMapColumnsAliases += (refObjectMap -> newColumnAliases);
                                } else {
                                    this.mapTermMapColumnsAliases += (refObjectMap -> List(alias));
                                }
                            }
                            result.addSelectItem(selectItem);
                            selectItem = MorphSQLSelectItem(join.parentColumnName, joinQueryAlias, dbType, null);
                            if (selectItem.getAlias() == null) {
                                val alias = selectItem.getTable() + "_" + selectItem.getColumn();
                                selectItem.setAlias(alias);
                                if (this.mapTermMapColumnsAliases.containsKey(refObjectMap)) {
                                    val oldColumnAliases = this.mapTermMapColumnsAliases(refObjectMap);
                                    val newColumnAliases = oldColumnAliases ::: List(alias);
                                    this.mapTermMapColumnsAliases += (refObjectMap -> newColumnAliases);
                                } else {
                                    this.mapTermMapColumnsAliases += (refObjectMap -> List(alias));
                                }
                            }
                            result.addSelectItem(selectItem);
                        }
                        // end of XR2RML
                    }
                }
            }
            logger.trace("Unfolded predicate-object map: " + result.toString.replaceAll("\n", " "))
        }

        try {
            val sliceString = this.properties.mapDataTranslationLimits.find(_._1.equals(triplesMapId));
            if (sliceString.isDefined) {
                val sliceLong = sliceString.get._2.toLong;
                result.setSlice(sliceLong);
            }

            val offsetString = this.properties.mapDataTranslationOffsets.find(_._1.equals(triplesMapId));
            if (offsetString.isDefined) {
                val offsetLong = offsetString.get._2.toLong;
                result.setOffset(offsetLong);
            }

        } catch {
            case e: Exception => {
                logger.error("Errors parsing LIMIT from properties file!")
            }
        }
        result;
    }

    def unfoldTriplesMap(triplesMap: R2RMLTriplesMap, subjectURI: String): IQuery = {

        logger.debug("Unfolding triples map " + triplesMap.toString + ", subjectURI: " + subjectURI)
        val logicalTable = triplesMap.logicalSource.asInstanceOf[xR2RMLLogicalSource];
        val resultAux = this.unfoldTriplesMap(triplesMap.id, logicalTable, triplesMap.subjectMap, triplesMap.predicateObjectMaps);
        val result = if (subjectURI != null) {
            val whereExpression = MorphRDBUtility.generateCondForWellDefinedURI(triplesMap.subjectMap, triplesMap, subjectURI, logicalTable.alias);
            if (whereExpression != null) {
                resultAux.addWhere(whereExpression)
                resultAux
            } else { null }
        } else { resultAux }
        result
    }

    def unfoldTriplesMap(triplesMap: R2RMLTriplesMap): IQuery = {
        this.unfoldTriplesMap(triplesMap, null);
    }

    override def unfoldConceptMapping(cm: MorphBaseClassMapping): IQuery = {
        this.unfoldTriplesMap(cm.asInstanceOf[R2RMLTriplesMap]);
    }

    override def unfoldConceptMapping(cm: MorphBaseClassMapping, subjectURI: String): IQuery = {
        this.unfoldTriplesMap(cm.asInstanceOf[R2RMLTriplesMap], subjectURI);
    }

    override def unfoldMappingDocument() = {
        val triplesMaps = this.md.classMappings
        val result = if (triplesMaps != null) {
            triplesMaps.flatMap(triplesMap => {
                try {
                    val triplesMapUnfolded = this.unfoldConceptMapping(triplesMap);
                    Some(triplesMapUnfolded);
                } catch {
                    case e: Exception => {
                        logger.error("error while unfolding triplesMap : " + triplesMap);
                        logger.error("error message = " + e.getMessage());
                        None
                    }
                }
            })
        } else {
            Nil
        }
        result;
    }

    override def unfoldSubject(cm: MorphBaseClassMapping): IQuery = {
        val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
        val logicalTable = triplesMap.logicalSource.asInstanceOf[xR2RMLLogicalSource];
        val subjectMap = triplesMap.subjectMap;
        val predicateObjectMaps = triplesMap.predicateObjectMaps;
        val id = triplesMap.id;
        val result = this.unfoldTriplesMap(id, logicalTable, subjectMap, null);
        return result;
    }

    def visit(logicalTable: xR2RMLLogicalSource): SQLLogicalTable = {
        val result = this.unfoldLogicalSource(logicalTable);
        result;
    }

    def visit(md: R2RMLMappingDocument): Collection[IQuery] = {
        val result = this.unfoldMappingDocument();
        result;
    }

    def visit(objectMap: R2RMLObjectMap): Object = {
        // TODO Auto-generated method stub
        null;
    }

    def visit(refObjectMap: R2RMLRefObjectMap): Object = {
        // TODO Auto-generated method stub
        null;
    }

    def visit(r2rmlTermMap: R2RMLTermMap): Object = {
        // TODO Auto-generated method stub
        null;
    }

    def visit(triplesMap: R2RMLTriplesMap): IQuery = {
        val result = this.unfoldTriplesMap(triplesMap);
        result;
    }
}

object MorphRDBUnfolder {
    def unfoldJoinConditions(
        pJoinConditions: Iterable[R2RMLJoinCondition],
        childTableAlias: String,
        joinQueryAlias: String,
        dbType: String): ZExpression = {

        val joinConditions = {
            if (pJoinConditions == null) { Nil }
            else { pJoinConditions }
        }

        val enclosedCharacter = Constants.getEnclosedCharacter(dbType);

        val joinConditionExpressions = joinConditions.map(
            joinCondition => {
                var childColumnName = joinCondition.childColumnName
                childColumnName = childColumnName.replaceAll("\"", enclosedCharacter);
                childColumnName = childTableAlias + "." + childColumnName;
                val childColumn = new ZConstant(childColumnName, ZConstant.COLUMNNAME);

                var parentColumnName = joinCondition.parentColumnName;
                parentColumnName = parentColumnName.replaceAll("\"", enclosedCharacter);
                parentColumnName = joinQueryAlias + "." + parentColumnName;
                val parentColumn = new ZConstant(parentColumnName, ZConstant.COLUMNNAME);

                new ZExpression("=", childColumn, parentColumn);
            })

        val result = if (joinConditionExpressions.size > 0) {
            MorphSQLUtility.combineExpresions(joinConditionExpressions, Constants.SQL_LOGICAL_OPERATOR_AND);
        } else {
            Constants.SQL_EXPRESSION_TRUE;
        }

        result;
    }
}