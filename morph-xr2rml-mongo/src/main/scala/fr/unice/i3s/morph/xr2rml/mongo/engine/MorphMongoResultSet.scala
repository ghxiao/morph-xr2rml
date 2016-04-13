package fr.unice.i3s.morph.xr2rml.mongo.engine

import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet

/**
 * Very basic class to store JSON documents resulting from a query to MongoDB
 * 
 * @author Franck Michel, I3S laboratory
 */
class MorphMongoResultSet(
        val resultSet: List[String]) extends MorphBaseResultSet {
}