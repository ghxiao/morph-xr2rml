package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class xR2RMLQuery(
    query: String,
    refFormulation: String,
    iterator: Option[String])
        extends xR2RMLLogicalSource(Constants.LogicalTableType.QUERY, refFormulation, iterator) {

    override def getValue(): String = { this.query; }
}
