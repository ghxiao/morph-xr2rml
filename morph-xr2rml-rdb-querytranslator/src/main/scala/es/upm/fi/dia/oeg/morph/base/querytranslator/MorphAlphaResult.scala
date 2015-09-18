package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable

class MorphAlphaResult(val alphaSubject: SQLLogicalTable, var alphaPredicateObjects: List[(SQLJoinTable, String)]) {
    override def toString = {
        (alphaSubject, alphaPredicateObjects).toString
    }
}