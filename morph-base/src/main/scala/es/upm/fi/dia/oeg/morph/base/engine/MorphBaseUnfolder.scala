package es.upm.fi.dia.oeg.morph.base.engine

import Zql.ZUtils
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.sql.ISqlQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import Zql.ZExpression
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLJoinCondition

abstract class MorphBaseUnfolder(md: R2RMLMappingDocument, properties: MorphProperties) {

    Constants.MAP_ZSQL_CUSTOM_FUNCTIONS.foreach(f => { ZUtils.addCustomFunction(f._1, f._2); })

    var dbType = Constants.DATABASE_MYSQL;

    @throws[MorphException]
    def unfoldConceptMapping(cm: R2RMLTriplesMap): GenericQuery

    def unfoldLogicalSource(logicalTable: xR2RMLLogicalSource): SQLLogicalTable

    def unfoldJoinConditions(
        joinConditions: Set[R2RMLJoinCondition],
        childTableAlias: String,
        joinQueryAlias: String,
        dbType: String): Object
}
