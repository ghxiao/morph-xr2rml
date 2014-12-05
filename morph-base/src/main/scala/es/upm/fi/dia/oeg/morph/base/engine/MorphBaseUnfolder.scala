package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import Zql.ZUtils
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.exception.MorphException

abstract class MorphBaseUnfolder(md: MorphBaseMappingDocument, properties: MorphProperties) {

    Constants.MAP_ZSQL_CUSTOM_FUNCTIONS.foreach(f => { ZUtils.addCustomFunction(f._1, f._2); })

    var dbType = Constants.DATABASE_MYSQL;

    @throws[MorphException]
    def unfoldConceptMapping(cm: MorphBaseClassMapping): GenericQuery;
}