package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import scala.collection.JavaConversions._
import java.util.Collection
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.base.Constants
import java.util.HashSet
import Zql.ZQuery
import Zql.ZSelectItem
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLLogicalTable
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTable
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSQLQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLJoinCondition
import Zql.ZExpression
import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLFromItem
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties

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
     */
    def unfoldLogicalTable(logicalTable: R2RMLLogicalTable): SQLLogicalTable = {
        val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
        val logicalTableType = logicalTable.logicalTableType;

        val result = logicalTableType match {
            case Constants.LogicalTableType.TABLE_NAME => {
                val logicalTableValueWithEnclosedChar = logicalTable.getValue().replaceAll("\"", dbEnclosedCharacter);
                val resultAux = new SQLFromItem(logicalTableValueWithEnclosedChar, Constants.LogicalTableType.TABLE_NAME);
                resultAux.databaseType = this.dbType;
                resultAux
            }
            case Constants.LogicalTableType.QUERY_STRING => {
                val sqlString = logicalTable.getValue().replaceAll("\"", dbEnclosedCharacter);
                val sqlString2 = if (!sqlString.endsWith(";")) {
                    sqlString + ";";
                } else {
                    sqlString
                }
                try {
                    MorphRDBUtility.toSQLQuery(sqlString2);
                } catch {
                    case e: Exception => {
                        logger.warn("Not able to parse the query, string will be used.");
                        val resultAux = new SQLFromItem(sqlString, Constants.LogicalTableType.QUERY_STRING);
                        resultAux.databaseType = this.dbType;
                        resultAux
                    }
                }
            }
            case _ => {
                logger.warn("Invalid logical table type");
                null;
            }
        }
        result;
    }

    /**
     * Return a list of select items corresponding to the columns referenced by the term map.
     * Nil in case of a constant-valued term-map, a list of one select item for a column-valued term map,
     * and a list of several select items for a template-valued term map.
     */
    def unfoldTermMap(termMap: R2RMLTermMap, logicalTableAlias: String): List[MorphSQLSelectItem] = {

        val result = if (termMap != null) {
            termMap.termMapType match {
                case Constants.MorphTermMapType.TemplateTermMap => {
                    val columns = termMap.getReferencedColumns();
                    if (columns != null) {
                        columns.map(
                            column => {
                                val selectItem = MorphSQLSelectItem.apply(column, logicalTableAlias, dbType);
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
                                selectItem
                            });
                    } else { Nil }
                }
                case Constants.MorphTermMapType.ColumnTermMap => {
                    val termColumnName = termMap.columnName;
                    val selectItem = MorphSQLSelectItem.apply(termColumnName, logicalTableAlias, dbType);

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
                case Constants.MorphTermMapType.ConstantTermMap => {
                    Nil;
                }
                case _ => {
                    throw new Exception("Invalid term map type!");
                }
            }

        } else {
            Nil
        }

        result
    }

    /**
     * Unfolding a triples map means to progressively build an SQL query by accumulating pieces:
     * (1) create the FROM clause with the logical table,
     * (2) for each column in the subject predicate and object maps, add items to the SELECT clause
     * (3) for each column in the parent triples map of each referencing object map, create items of the SELECT clause
     * (4) for each join condition, add an SQL WHERE condition and an alias in the FROM clause for the parent table
     * (5) xR2RML: for each column of each join condition, add items to the SELECT clause
     * 
     * @return an SQLQuery (IQuery) describing the actual SQL query to be run against the RDB
     */
    def unfoldTriplesMap(
        triplesMapId: String,
        logicalTable: R2RMLLogicalTable,
        subjectMap: R2RMLSubjectMap,
        poms: Collection[R2RMLPredicateObjectMap]): IQuery = {

        val result = new SQLQuery();
        result.setDatabaseType(this.dbType);

        // UNFOLD LOGICAL TABLE: build an SQL query that represents the logical table
        val logicalTableUnfolded: SQLFromItem = logicalTable match {
            case _: R2RMLTable => {
                this.unfoldLogicalTable(logicalTable).asInstanceOf[SQLFromItem];
            }
            case _: R2RMLSQLQuery => {
                val logicalTableAux = this.unfoldLogicalTable(logicalTable)
                logicalTableAux match {
                    case _: SQLQuery => {
                        val zQuery = this.unfoldLogicalTable(logicalTable).asInstanceOf[ZQuery];
                        val resultAux = new SQLFromItem(zQuery.toString(), Constants.LogicalTableType.QUERY_STRING);
                        resultAux.databaseType = this.dbType
                        resultAux
                    }
                    case sqlFromItem: SQLFromItem => { sqlFromItem; }
                    case _ => { null }
                }
            }
            case _ => { null }
        }
        val logicalTableAlias = logicalTableUnfolded.generateAlias();
        logicalTable.alias = logicalTableAlias;
        val logicalTableUnfoldedJoinTable = new SQLJoinTable(logicalTableUnfolded, null, null);
        result.addFromItem(logicalTableUnfoldedJoinTable);

        // Unfold subject map
        val subjectMapSelectItems = this.unfoldTermMap(subjectMap, logicalTableAlias);
        result.addSelectItems(subjectMapSelectItems);

        if (poms != null) {
            for (pom <- poms) {
                //UNFOLD PREDICATEMAP
                val predicateMaps = pom.predicateMaps;
                if (predicateMaps != null && !predicateMaps.isEmpty()) {
                    val predicateMap = pom.getPredicateMap(0);
                    val predicateMapSelectItems = this.unfoldTermMap(predicateMap, logicalTableAlias);
                    result.addSelectItems(predicateMapSelectItems);
                }

                //UNFOLD OBJECTMAP
                val objectMaps = pom.objectMaps;
                if (objectMaps != null && !objectMaps.isEmpty()) {
                    val objectMap = pom.getObjectMap(0);
                    val objectMapSelectItems = this.unfoldTermMap(objectMap, logicalTableAlias);
                    result.addSelectItems(objectMapSelectItems);
                }

                //UNFOLD REFOBJECTMAP
                val refObjectMaps = pom.refObjectMaps;
                if (refObjectMaps != null && !refObjectMaps.isEmpty()) {
                    val refObjectMap = pom.getRefObjectMap(0);
                    if (refObjectMap != null) {
                        val parentTriplesMap = this.md.getParentTriplesMap(refObjectMap);
                        val parentLogicalTable = parentTriplesMap.getLogicalTable();
                        if (parentLogicalTable == null) {
                            val errorMessage = "Parent logical table is not found for RefObjectMap : " + pom.getMappedPredicateName(0);
                            throw new Exception(errorMessage);
                        }
                        val sqlParentLogicalTable = this.unfoldLogicalTable(parentLogicalTable.asInstanceOf[R2RMLLogicalTable]);
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

                        /** @note XR2RML:
                         *  The joined columns are not necessarily referenced in the term maps. 
                         *  However in the case of xR2RML, if the joined columns do not contain simple values but 
                         *  structured values (e.g. an XML value that must be evaluated by an XPath expression), then
                         *  the join operation cannot be done by the database itself (in the SQL query) but afterwards in
                         *  the code. Therefore we add the joined columns in the select clause to have those values in case
                         *  we must make the join */
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
                logger.error("errors parsing LIMIT from properties file!")
            }
        }
        result;
    }

    def unfoldTriplesMap(triplesMap: R2RMLTriplesMap, subjectURI: String): IQuery = {
        val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
        val subjectMap = triplesMap.subjectMap;
        val predicateObjectMaps = triplesMap.predicateObjectMaps;
        val triplesMapId = triplesMap.id;

        val resultAux = this.unfoldTriplesMap(triplesMapId, logicalTable, subjectMap, predicateObjectMaps);
        val result = if (subjectURI != null) {
            val whereExpression = MorphRDBUtility.generateCondForWellDefinedURI(subjectMap, triplesMap, subjectURI, logicalTable.alias);
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
        val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
        val subjectMap = triplesMap.subjectMap;
        val predicateObjectMaps = triplesMap.predicateObjectMaps;
        val id = triplesMap.id;
        val result = this.unfoldTriplesMap(id, logicalTable, subjectMap, null);
        return result;
    }

    def visit(logicalTable: R2RMLLogicalTable): SQLLogicalTable = {
        val result = this.unfoldLogicalTable(logicalTable);
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
        parentTableAlias: String,
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
                childColumnName = parentTableAlias + "." + childColumnName;
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