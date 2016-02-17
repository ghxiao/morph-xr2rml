package es.upm.fi.dia.oeg.morph.rdb.engine

import java.util.Collection

import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.JavaConversions.setAsJavaSet

import org.apache.log4j.Logger

import Zql.ZConstant
import Zql.ZExpression
import Zql.ZQuery
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.path.MixedSyntaxPath
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.base.sql.ISqlQuery
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.SQLFromItem
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLJoinCondition
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLTable
import es.upm.fi.dia.oeg.morph.rdb.MorphRDBUtility

class MorphRDBUnfolder(md: R2RMLMappingDocument, properties: MorphProperties)
        extends MorphBaseUnfolder(md, properties) {

    val logger = Logger.getLogger(this.getClass().getName());

    /** List the SQL aliases of all columns referenced by each term map of the triples map */
    var mapTermMapColumnsAliases: Map[Object, List[String]] = Map.empty;

    /** List the SQL aliases of referencing object maps of the triples map */
    var mapRefObjectMapAlias: Map[R2RMLRefObjectMap, String] = Map.empty;

    private def getAliases(termMapOrRefObjectMap: Object): Collection[String] = {
        if (this.mapTermMapColumnsAliases.get(termMapOrRefObjectMap).isDefined) {
            this.mapTermMapColumnsAliases(termMapOrRefObjectMap);
        } else {
            null
        }
    }

    /**
     * Build an instance of SQLLogicalTable, that may be either an
     * SQLFromItem in case of a logical table with a table name,
     * or an SQLQuery in case of a logical table with a query string
     *
     * @return instance of SQLFromItem or SQLQuery
     */
    @throws[MorphException]
    override def unfoldLogicalSource(logicalTable: xR2RMLLogicalSource): SQLLogicalTable = {
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
                        logger.warn("Not able to parse the query, string will be used: " + e.getMessage);
                        val resultAux = new SQLFromItem(sqlString, Constants.LogicalTableType.QUERY);
                        resultAux.databaseType = this.dbType;
                        resultAux
                    }
                }
            }
            case _ => { throw new MorphException("Invalid logical table type" + logicalTable.logicalTableType) }
        }
        result;
    }

    /**
     * Return a list of select items corresponding to the columns referenced by the term map.
     * Nil in case of a constant-valued term-map, a list of one select item for a column-valued term map,
     * and a list of several select items for a template-valued term map.
     */
    @throws[MorphException]
    private def unfoldTermMap(termMap: R2RMLTermMap, logicalTableAlias: String): List[MorphSQLSelectItem] = {

        val result =
            if (termMap != null) {
                termMap.termMapType match {

                    case Constants.MorphTermMapType.ConstantTermMap => { Nil }

                    case Constants.MorphTermMapType.ColumnTermMap => {
                        val selectItem = MorphSQLSelectItem(termMap.columnName, logicalTableAlias, dbType);
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
                                val selectItem = MorphSQLSelectItem(column, logicalTableAlias, dbType);
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
                                val selectItem = MorphSQLSelectItem(column, logicalTableAlias, dbType);
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

                    case _ => { throw new MorphException("Invalid term map type: " + termMap.termMapType) }
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
     * (5) xR2RML: in case of mixed syntax path reference (including JSONPath, CSV etc.) the join cannot fully be performed in SQL
     * by the database, therefore, for each column of such join condition, add items to the SELECT clause.
     *
     * @return an SQLQuery (IQuery) describing the actual SQL query to be run against the RDB
     */
    private def unfoldTriplesMap(
        triplesMapId: String,
        logicalSrc: xR2RMLLogicalSource,
        subjectMap: R2RMLSubjectMap,
        poms: Collection[R2RMLPredicateObjectMap]): ISqlQuery = {

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
            case _ => { throw new MorphException("Unknown logical table/source type: " + logicalSrc) }
        }

        // ----- Create an alias for the sub-query in the FROM clause
        val logicalTableAlias = logicalSrcUnfolded.generateAlias()
        logicalSrc.alias = logicalTableAlias
        val logTabUnfoldedJoinTab = new SQLJoinTable(logicalSrcUnfolded)
        result.addFromItem(logTabUnfoldedJoinTab)
        logger.trace("Unfolded logical source: " + result.toString.replaceAll("\n", ""))

        // ----- Unfold subject map: add select for columns referenced in subject map
        val subjectMapSelectItems = this.unfoldTermMap(subjectMap, logicalTableAlias);
        result.addSelectItems(subjectMapSelectItems);
        logger.debug("Unfolded subject map: " + result.toString.replaceAll("\n", ""))

        // ----- Unfold predicate-object maps
        if (poms != null) {
            for (pom <- poms) {
                // Unfold all PredicateMaps of the current predicate-object map: add select for columns referenced in predicate map 
                if (pom.predicateMaps != null)
                    for (pm <- pom.predicateMaps) {
                        val selectItems = this.unfoldTermMap(pm, logicalTableAlias);
                        result.addSelectItems(selectItems);
                    }

                // Unfold all ObjectMaps of the current predicate-object map: add select for columns referenced in object map
                if (pom.objectMaps != null) {
                    for (om <- pom.objectMaps) {
                        val selectItems = this.unfoldTermMap(om, logicalTableAlias);
                        result.addSelectItems(selectItems);
                    }
                }

                // Unfold RefObjectMaps
                if (pom.refObjectMaps != null) {
                    for (refObjectMap <- pom.refObjectMaps) {
                        if (refObjectMap != null) {

                            val parentTM = this.md.getParentTriplesMap(refObjectMap);

                            // Create an alias for the parent SQL query
                            val sqlParentLogSrc = this.unfoldLogicalSource(parentTM.logicalSource.asInstanceOf[xR2RMLLogicalSource]);
                            val joinQueryAlias = sqlParentLogSrc.generateAlias();
                            sqlParentLogSrc.setAlias(joinQueryAlias);
                            this.mapRefObjectMapAlias += (refObjectMap -> joinQueryAlias);
                            pom.alias = joinQueryAlias;

                            // Add select for columns referenced in the parent triples map's subject map
                            val parentSubCols = parentTM.subjectMap.getReferencedColumns;
                            if (parentSubCols != null) {
                                for (parentSubCol <- parentSubCols) {
                                    val selectItem = MorphSQLSelectItem(parentSubCol, joinQueryAlias, dbType, null);
                                    if (selectItem.getAlias() == null) {
                                        val alias = selectItem.getTable() + "_" + selectItem.getColumn();
                                        selectItem.setAlias(alias)
                                        saveAlias(refObjectMap, alias)
                                    }
                                    result.addSelectItem(selectItem);
                                }
                            }

                            val joinConditions = refObjectMap.joinConditions;

                            /**
                             *  If at least one of the joined columns is not a simple column reference, but contains mixed syntax
                             *  (e.g. with XPath or JSONPath) then the database cannot evaluate the join. Instead we will have
                             *  to do it later. And to do that, we must make sure that the columns to join will be in the result set.
                             *  Therefore, we add the columns in the join conditions to the select clause of the SQL query.
                             */
                            for (join <- joinConditions) {

                                val childMsp = MixedSyntaxPath(join.childRef, Constants.xR2RML_REFFORMULATION_COLUMN)
                                val parentMsp = MixedSyntaxPath(join.parentRef, Constants.xR2RML_REFFORMULATION_COLUMN)

                                if (!childMsp.isSimpleColumnExpression || !parentMsp.isSimpleColumnExpression) {

                                    // Add a select clause for the child column reference
                                    var selectItem = MorphSQLSelectItem(childMsp.getReferencedColumn.get, logicalTableAlias, dbType, null);
                                    if (selectItem.getAlias() == null) {
                                        val alias = selectItem.getTable() + "_" + selectItem.getColumn();
                                        selectItem.setAlias(alias);
                                    }
                                    result.addSelectItem(selectItem);

                                    // Add a select clause for the parent column reference
                                    selectItem = MorphSQLSelectItem(parentMsp.getReferencedColumn.get, joinQueryAlias, dbType, null);
                                    if (selectItem.getAlias() == null) {
                                        val alias = selectItem.getTable() + "_" + selectItem.getColumn();
                                        selectItem.setAlias(alias);
                                    }
                                    result.addSelectItem(selectItem);
                                }
                            }

                            // Add a left join clause for columns in join conditions of RefObjectMaps
                            val onExpression = this.unfoldJoinConditions(joinConditions, logicalTableAlias, joinQueryAlias, dbType).asInstanceOf[ZExpression];
                            val joinQuery = new SQLJoinTable(sqlParentLogSrc, Constants.JOINS_TYPE_LEFT, onExpression);
                            result.addFromItem(joinQuery);
                        }
                    }
                }
            }
            logger.debug("Unfolded predicate-object map: " + result.toString.replaceAll("\n", " "))
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
            case e: MorphException => {
                logger.error("Errors parsing LIMIT from properties file!")
                e.printStackTrace()
            }
        }
        result;
    }

    def unfoldTriplesMap(triplesMap: R2RMLTriplesMap, subjectURI: String): ISqlQuery = {
        val logicalTable = triplesMap.getLogicalSource().asInstanceOf[xR2RMLLogicalSource];
        val subjectMap = triplesMap.subjectMap;
        val predicateObjectMaps = triplesMap.predicateObjectMaps;
        val triplesMapId = triplesMap.id;

        val resultAux = this.unfoldTriplesMap(triplesMapId, logicalTable, subjectMap, predicateObjectMaps);
        val result = if (subjectURI != null) {
            val whereExpression = MorphRDBUtility.generateCondForWellDefinedURI(
                subjectMap, triplesMap, subjectURI, logicalTable.alias);
            if (whereExpression != null) {
                resultAux.addWhere(whereExpression);
                resultAux;
            } else {
                null;
            }
        } else {
            resultAux;
        }
        result;
    }

    def unfoldTriplesMap(triplesMap: R2RMLTriplesMap): ISqlQuery = {
        this.unfoldTriplesMap(triplesMap, null);
    }

    /**
     * Save an alias corresponding to a reference object map to this.mapTermMapColumnsAliases
     */
    def saveAlias(refObjectMap: R2RMLRefObjectMap, alias: String) = {
        if (this.mapTermMapColumnsAliases.containsKey(refObjectMap)) {
            val oldColumnAliases = this.mapTermMapColumnsAliases(refObjectMap);
            val newColumnAliases = oldColumnAliases ::: List(alias);
            this.mapTermMapColumnsAliases += (refObjectMap -> newColumnAliases);
        } else
            this.mapTermMapColumnsAliases += (refObjectMap -> List(alias));
    }

    /**
     * Entry point of the unfolder in the data materialization case
     */
    override def unfoldConceptMapping(cm: R2RMLTriplesMap): GenericQuery = {

        val triplesMap = cm.asInstanceOf[R2RMLTriplesMap]
        logger.debug("Unfolding triples map " + triplesMap.toString)
        val logicalTable = triplesMap.logicalSource.asInstanceOf[xR2RMLLogicalSource];
        val resultAux = this.unfoldTriplesMap(triplesMap.id, logicalTable, triplesMap.subjectMap, triplesMap.predicateObjectMaps);

        logger.info("Query for triples map " + cm.id + ": " + resultAux.print(true).replaceAll("\n", " "))

        new GenericQuery(None, Constants.DatabaseType.Relational, resultAux)
    }

    def unfoldMappingDocument() = {
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
        } else
            Nil
        result.toList
    }

    /**
     * Build a set of SQL join conditions like "child = parent" to reflect a RefObjectMap's joins conditions.
     * For each join condition, if either the child or parent references is not a regular
     * R2RML column reference (i.e. if it has a mixed-syntax path with more than one path constructor),
     * then no join condition is returned: in that case, the join will have to be computed by the xR2RML processor.
     * But the child and parent columns are then added as select items.
     */
    override def unfoldJoinConditions(
        joinConditions: Set[R2RMLJoinCondition],
        childTableAlias: String,
        joinQueryAlias: String,
        dbType: String): Object = {

        val joinConds =
            if (joinConditions == null) Nil
            else joinConditions

        val enclosedChar = Constants.getEnclosedCharacter(dbType);
        val joinExpressions = joinConds.flatMap(join => {

            // If both the child and parent references are pure column references (without other path constructor)
            // then the join is made by the database. Otherwise we'll have to do the job ourselves.
            var childRef = join.childRef
            var parentRef = join.parentRef
            if (MixedSyntaxPath(childRef, Constants.xR2RML_REFFORMULATION_COLUMN).isSimpleColumnExpression &&
                MixedSyntaxPath(parentRef, Constants.xR2RML_REFFORMULATION_COLUMN).isSimpleColumnExpression) {

                childRef = childTableAlias + "." + childRef.replaceAll("\"", enclosedChar)
                val childColumn = new ZConstant(childRef, ZConstant.COLUMNNAME)

                parentRef = joinQueryAlias + "." + parentRef.replaceAll("\"", enclosedChar)
                val parentColumn = new ZConstant(parentRef, ZConstant.COLUMNNAME)

                Some(new ZExpression("=", childColumn, parentColumn))
            } else
                None
        })

        val result = if (joinExpressions.size > 0) {
            MorphSQLUtility.combineExpresions(joinExpressions, Constants.SQL_LOGICAL_OPERATOR_AND);
        } else {
            Constants.SQL_EXPRESSION_TRUE;
        }

        result;
    }
}