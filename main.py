from GenericQuueryProcessor import *


if __name__ == "__main__":

    rel_path = "relational.db"
    rel_qp = rel.RelationalQueryProcessor()
    rel_qp.setDbPath(rel_path)

    #here I am checking the is_publication_in_db query in the RelationalQueryProcessor
    query_anita = rel_qp.is_publication_in_db('doi:10.1162/qss_a_00112')
    print("publication in relational db", query_anita)
    #false case: doi:10.1016/j.cirpj.2018.06.002

    #here I am checking the is_publication_in_db query in the TriplestoreQueryProcessor
    grp_endpoint = "http://10.201.34.226:9999/blazegraph/sparql"
    grp_qp = TriplestoreQueryProcessor()
    grp_qp.setEndpointUrl(grp_endpoint)
    Q_Anita = grp_qp.is_publication_in_db("doi:10.1007/s00521-020-05491-5")
    print("publication in triplestore db", Q_Anita) 
    #false case:  doi:10.1162/qss_a_00112
    


    generic = GenericQueryProcessor()
    generic.addQueryProcessor(rel_qp)
    h_index = generic.compute_h_index('0000-0001-5506-523')
    print("H-index for the author:", h_index)
    print('hello 2')
