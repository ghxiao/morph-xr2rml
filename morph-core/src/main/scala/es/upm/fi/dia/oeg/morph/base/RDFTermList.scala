package es.upm.fi.dia.oeg.morph.base

class RDFTermList(val v: List[RDFTerm]) extends RDFTerm(v) {

    override val isList = true

    def getMembers = v
    
    override def equals(a: Any): Boolean = {
        this.getClass == a.getClass && value == a.asInstanceOf[RDFTermList].value
    }

    override def hashCode(): Int = {
        this.getClass.hashCode + value.hashCode()
    }    
}