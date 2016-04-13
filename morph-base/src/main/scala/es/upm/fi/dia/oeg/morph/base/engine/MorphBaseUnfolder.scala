package es.upm.fi.dia.oeg.morph.base.engine

import Zql.ZUtils
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLJoinCondition
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

abstract class MorphBaseUnfolder(factory: IMorphFactory) {

    Constants.MAP_ZSQL_CUSTOM_FUNCTIONS.foreach(f => { ZUtils.addCustomFunction(f._1, f._2); })

    var dbType = factory.getProperties.databaseType;

    def unfoldTriplesMap(tm: R2RMLTriplesMap): GenericQuery

    def unfoldJoinConditions(
        joinConditions: Set[R2RMLJoinCondition],
        childTableAlias: String,
        joinQueryAlias: String,
        dbType: String): Object
}
