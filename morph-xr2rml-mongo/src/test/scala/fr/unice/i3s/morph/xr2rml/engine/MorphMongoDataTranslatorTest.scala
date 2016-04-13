package fr.unice.i3s.morph.xr2rml.engine

import org.junit.Assert.assertEquals
import org.junit.Test

import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataTranslator

/**
 * @author Franck Michel, I3S laboratory
 *
 */
class MorphMongoDataTranslatorTest {

    @Test def parseQueryString() = {

        val query = "db.testData.find({ 'a': { $exists: true} })"

        var tokens = query.split("\\.")
        if (!tokens(0).equals("db")) {
            println("Invalid query string: " + query)
        } else {
            val collection = tokens(1)
            println("Collection: " + collection)

            tokens = query.split("\\(")
            for (tok <- tokens)
                println(tok)
            val queryStr = tokens(1).substring(0, tokens(1).length - 1)
            println("queryStr: [" + queryStr + "]")
        }
    }
}