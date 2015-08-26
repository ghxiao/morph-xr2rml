package es.upm.fi.dia.oeg.morph.base.engine

import Zql.ZUtils
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

abstract class MorphBaseUnfolder(md: R2RMLMappingDocument, properties: MorphProperties) {

    Constants.MAP_ZSQL_CUSTOM_FUNCTIONS.foreach(f => { ZUtils.addCustomFunction(f._1, f._2); })

    var dbType = Constants.DATABASE_MYSQL;

    @throws[MorphException]
    def unfoldConceptMapping(cm: R2RMLTriplesMap): GenericQuery;
}