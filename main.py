#RUN THE SOFTWARE AND ANITA'S FUNCTIONS 
from ModelClasses import *
from GenericQuueryProcessor import *

pub1 = Publication(["id-pub-4000"], 2019, "Titolo Pubblicazione Citata", ["Autore2"], "Venue 1", [])
pub2 = Publication(["id-pub-1000", "id-pub-2000"], 2020, "Titolo Pubblicazione Prova", ["Autore", "Autore2"], "Venue 1", [pub1])


lista1 = [pub1, pub2, pub1, pub2, pub1]
lista2 = [pub2, pub2, pub1, pub1]


if __name__ == "__main__":
    #///////////RELATIONAL//////////
    rel_path = "relational.db"
    rel_qp = rel.RelationalQueryProcessor()
    rel_qp.setDbPath(rel_path)

    #here I am checking the is_publication_in_db query in the RelationalQueryProcessor
    query_anita = rel_qp.is_publication_in_db('doi:10.1162/qss_a_00112')
    print("publication in relational db", query_anita)
    #false case: doi:10.1016/j.cirpj.2018.06.002

    #here i am running the additional query that I have implemented for the h_index method
    query_h = rel_qp.count_citations('doi:10.1162/qss_a_00023')
    print("the number of citations of this doi is:", query_h)


    #*///////////TRIPLESTORE///////
    #here I am checking the is_publication_in_db query in the TriplestoreQueryProcessor
    grp_endpoint = "http://192.168.1.53:9999/blazegraph/sparql"
    grp_qp = TriplestoreQueryProcessor()
    grp_qp.setEndpointUrl(grp_endpoint)
    Q_Anita = grp_qp.is_publication_in_db("doi:10.1007/s00521-020-05491-5")
    print("publication in triplestore db", Q_Anita) 
    #false case:  doi:10.1162/qss_a_00112

    #here I run an additional query for the h_index method
    Q_index = grp_qp.count_citations("doi:10.1093/nar/gkz997")
    print("the number of citations of this doi is:", Q_index)

  
    #*//////GENERIC////////
    generic = GenericQueryProcessor()
    generic.addQueryProcessor(rel_qp)
    h_index = generic.compute_h_index('0000-0001-5506-523')
    print("H-index for the author:", h_index)


    dup = generic.remove_dup(lista1, lista2)
    print(dup)

    #list_without_duplicates = generic.remove_duplicates(lista1, lista2)
    #print(list_without_duplicates)

   



    

