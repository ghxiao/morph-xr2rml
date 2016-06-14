package es.upm.fi.dia.oeg.morph.base.exception

/**
 * @author Franck Michel, I3S laboratory
 */ 
class MorphException(message: String, cause: Throwable) extends Exception(message, cause) {

    def this(message: String) = this(message, null)

}