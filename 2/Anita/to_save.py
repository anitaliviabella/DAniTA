#to access blazegraph
#cd ~/Desktop/jar
#java -server -Xmx1g -jar blazegraph.jar
#http://127.0.0.1:9999/blazegraph/


#RELATIONALQUERYPROCESSOR
#------------------------------------ANITA METHOD IS HERE-----------------------------#
#This method has been implemented by Anita Liviabella, following the professor guidelines order to retake the exam. 
#The purpose of the method of the is_publication_in_db(self, pub_id) method in the RelationalQueryProcessor class is to take in input a string and return a boolean: True if the publication identified by the input id is included in the dababase, False otherwise.
'''class RelationalQueryProcessor():

    def is_publication_in_db(self, pub_id):
        if type(pub_id) == str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query_anita = "SELECT id FROM JournalArticleTable WHERE id = ? UNION SELECT id FROM BookChapterTable WHERE id = ? UNION SELECT id FROM ProceedingsPaperTable WHERE id = ?;"
        
                cur.execute(query_anita, (pub_id, pub_id, pub_id))
                result = cur.fetchall()
        
            if result:
                return True
            else:
                return False
        else:
            raise ValueError("The input parameter is not a string!")


#test
query_anita = z.is_publication_in_db("doi:10.1162/qss_a_00112")
print("is_publication_in_db Query\n", query_anita)

#------------------------
#TRIPLESTOREDATAPROCESSOR
class TriplestoreDataProcessor()
    def is_publication_in_db(self, pub_id):
        endpoint = self.getEndpointURL()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>

        SELECT ?publication
        WHERE {{
        ?publication rdf:type fabio:Expression ;
                 schema:identifier "{pub_id}".
         }}
        """
        result = get(endpoint, query, True)

        return not result.empty

#test
Q_Anita = z.is_publication_in_db("doi:10.1016/j.websem.2014.03.003")
print(Q_Anita)
'''