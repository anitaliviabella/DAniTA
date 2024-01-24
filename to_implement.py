'''# Classes RelationalQueryProcessor and TriplestoreQueryProcessor

Add the following method to both the classes:

def is_publication_in_db(pub_id : str) : boolean

It returns True if the publication identified by the input id is included in the dababase, False otherwise.'''





'''relational, file TEST_extraRelationalClasses.py
    def is_publication_in_db(self, pub_id):
        if type(pub_id) == str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT id FROM JournalArticleTable WHERE id = ? UNION SELECT id FROM BookChapterTable WHERE id = ? UNION SELECT id FROM ProceedingsPaperTable WHERE id = ?;"
        
                cur.execute(query, (pub_id, pub_id, pub_id))
                result = cur.fetchall()
        
                if result:
                    return True
                else:
                    return False
        else:
            raise ValueError("The input parameter is not a string!")
#test: 
query_anita = z.is_publication_in_db("doi:10.1162/qss_a_00112")
print("is_publication_in_db Query\n", query_anita)


'''


''' graph: graphData_Manager.py, remember to change the blazegrpah url!

 def is_publication_in_db(self, doi):
        endpoint = self.getEndpointUrl()
        # Check if pub_id is a string
        if not isinstance(doi, str):
                raise ValueError("pub_id must be a string")

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
        result = get(endpoint, query.format(pub_id = doi), True)
        return not result.empty
    
    #test:
    Q_Anita = grp_qp.is_publication_in_db("doi:10.1007/s00521-020-05491-5")
    print(Q_Anita)

'''




'''# Class GenericQueryProcessor

def compute_h_index(author_id : str) : int

It returns a non-negative integer that is the maximum value 'h' such that the author identified by the input id has published 'h' papers that have each been cited at least 'h' times.




    def compute_h_index(self, author_id):
        print(f"Inside compute_h_index method for author_id: {author_id}")
        if author_id != str:
            return None
        else:
        # Get the publications for the given author_id
            publications = self.getPublicationsByAuthorId(author_id)

        # Filter out None publications and get the list of citation counts
            citation_counts = [len(publication.getCitedPublications()) for publication in publications if publication]

        # Sort the citation counts in descending order
            citation_counts.sort(reverse=True)

        # Calculate h-index
            h_index = 0
            for i, count in enumerate(citation_counts):
                if i + 1 <= count:
                    h_index += 1
                else:
                    break

        return h_index




def remove_duplicates(l1 : list[Publication], l2 : list[Publication]) : list[Publication]

It takes in input two different list of publications, and returns a new list that contains the union of the publication in both list (removing the duplicates).'''


'''cose da fare: fare test aggiuntivi, inoltre scrivere una descrizione dettagliata del metodo.'''