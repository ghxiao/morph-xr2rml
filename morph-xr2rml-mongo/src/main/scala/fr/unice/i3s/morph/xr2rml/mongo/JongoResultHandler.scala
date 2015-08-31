package fr.unice.i3s.morph.xr2rml.mongo

import org.jongo.ResultHandler
import com.mongodb.DBObject

/**
 * Most simple Jongo result handler to return the JSON serialization of the result
 */
class JongoResultHandler extends ResultHandler[String] {

    @Override
    def map(result: DBObject): String = {
        result.toString()
    }
}