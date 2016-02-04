package fr.unice.i3s.morph.xr2rml.mongo.engine

import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet

class MorphMongoResultSet(
        val resultSet: Iterator[String]) extends MorphBaseResultSet {
}