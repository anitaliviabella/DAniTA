import sqlite3 as sql3
#Extrapolated code from other files.

''' Classes RelationalQueryProcessor and TriplestoreQueryProcessor
Add the following method to both the classes:

def is_publication_in_db(pub_id : str) : boolean
It returns True if the publication identified by the input id is included in the dababase, False otherwise.'''


#RELATIONALQUERYPROCESSOR
#It returns True if the publication identified by the input id is included in the dababase, False otherwise.
'''From extraRelationalClasses.py'''
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

'From TEST_extraRelationalClasses.py'
'''TEST:
query_anita = z.is_publication_in_db("doi:10.1162/qss_a_00112") --returns True because it is in the db.
print("is_publication_in_db Query\n", query_anita)'''


'''from genericQueryProcessor.py'''
#GENERICQUERYPROCESSOR  class
#It returns a non-negative integer that is the maximum value 'h' such that the author identified by the input id has published 'h' papers that have each been cited at least 'h' times.
def compute_h_index(self, author_id):
        if type(author_id) != str:
            return "The input parameter is not a string!"

        citations_count = {}  
        for item in self.queryProcessor:
            result_DF = item.getPublicationsByAuthorId(author_id)
            for idx, row in result_DF.iterrows():
                citations = row["num_citations"]
                if citations not in citations_count:
                    citations_count[citations] = 1
                else:
                    citations_count[citations] += 1

        h_index = 0
        total_papers = 0

        # Iterate through citation counts to find H-index
        for citations in sorted(citations_count.keys(), reverse=True):
            total_papers += citations_count[citations]
            if total_papers >= citations:
                h_index = citations

        return h_index
