import pandas as pd
import sqlite3 as sql3
import json 

from ModelClasses import QueryProcessor
from auxiliary import readCSV, readJSON

class RelationalProcessor(object):
    def __init__(self):
        # 'db_path' is name we use for the database path 
        self.db_path = ""

    def getDbpath(self):
        if self.db_path == "":
            return "DbPath is currently unset" + self.db_path
        else:
            return self.db_path
        
    def setDbpath(self, new_db_path):
        if new_db_path is str:
            self.db_path = new_db_path
            return True
        else:
            return False
    
# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
    

class RelationalDataProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()
    
    def uploadData(self,filepath):
        if type(filepath) != str:
            return False
        else:
            # =============== CSV UPLOAD DATA ===============
            if filepath.endswith(".csv"):
                df_publications = pd.read_csv(filepath,na_filter=False)

                # =============== PUBLICATION DATAFRAMES ===============

                journal_article_df = pd.DataFrame({
                    "issue": pd.Series(dtype="str"),
                    "volume": pd.Series(dtype="str"),
                    "publication_year": pd.Series(dtype="int"),
                    "title": pd.Series(dtype="str"),
                    "publication_venue": pd.Series(dtype="str"),
                    "id": pd.Series(dtype="str"),
                    "pub_type": pd.Series(dtype="str")
                })

                book_chapter_df = pd.DataFrame({
                    "chapter_number": pd.Series(dtype="str"),
                    "publication_year": pd.Series(dtype="int"),
                    "title": pd.Series(dtype="str"),
                    "publication_venue": pd.Series(dtype="str"),
                    "id": pd.Series(dtype="str"),
                    "pub_type": pd.Series(dtype="str")
                })

                proceeding_paper_df = pd.DataFrame({
                    "publication_year": pd.Series(dtype="int"),
                    "title": pd.Series(dtype="str"),
                    "publication_venue": pd.Series(dtype="str"),
                    "id": pd.Series(dtype="str"),
                    "pub_type": pd.Series(dtype="str")
                })

                journal_article_df['issue'] = df_publications[df_publications['type']== "journal-article"]['issue'].astype('str')
                journal_article_df['volume'] = df_publications[df_publications['type']== "journal-article"]['volume'].astype('str')
                journal_article_df['publication_year'] = df_publications[df_publications['type']== "journal-article"]['publication_year'].astype('int')
                journal_article_df['title'] = df_publications[df_publications['type']== "journal-article"]['title'].astype('str')
                journal_article_df['publication_venue'] = df_publications[df_publications['type']== "journal-article"]['publication_venue'].astype('str')
                journal_article_df['id'] = df_publications[df_publications['type']== "journal-article"]['id'].astype('str')
                journal_article_df['pub_type'] = df_publications[df_publications['type']== "journal-article"]['type'].astype('str')
                journal_article_df.replace(to_replace="nan",value="")

                book_chapter_df['publication_year'] = df_publications[df_publications['type']== "book-chapter"]['publication_year'].astype('int')
                book_chapter_df['title'] = df_publications[df_publications['type']== "book-chapter"]['title'].astype('str')
                book_chapter_df['chapter_number'] = df_publications[df_publications['type']== "book-chapter"]['chapter'].astype('str')
                book_chapter_df['publication_venue'] = df_publications[df_publications['type']== "book-chapter"]['publication_venue'].astype('str')
                book_chapter_df['id'] = df_publications[df_publications['type']== "book-chapter"]['id'].astype('str')
                book_chapter_df['pub_type'] = df_publications[df_publications['type']== "book-chapter"]['type'].astype('str')
                book_chapter_df.replace(to_replace="nan",value="")
                

                proceeding_paper_df['publication_year'] = df_publications[df_publications['type']== "proceedings-paper"]['publication_year'].astype('int')
                proceeding_paper_df['title'] = df_publications[df_publications['type']== "proceedings-paper"]['title'].astype('str')
                proceeding_paper_df['publication_venue'] = df_publications[df_publications['type']== "proceedings-paper"]['publication_venue'].astype('str')
                proceeding_paper_df['id'] = df_publications[df_publications['type'] == "proceedings-paper"]['id'].astype('str')
                proceeding_paper_df['pub_type'] = df_publications[df_publications['type']== "proceedings-paper"]['type'].astype('str')
                proceeding_paper_df.replace(to_replace="nan",value="")

                # =============== VENUES DATAFRAMES ===============

                journal_df = pd.DataFrame({
                    "name_venue": pd.Series(dtype="str"),
                    "publisher": pd.Series(dtype="str"),
                    "id_venue": pd.Series(dtype="str"),
                    "venue_type": pd.Series(dtype="str")
                })

                book_df = pd.DataFrame({
                    "name_venue": pd.Series(dtype="str"),
                    "publisher": pd.Series(dtype="str"),
                    "id_venue": pd.Series(dtype="str"),
                    "venue_type": pd.Series(dtype="str")
                })

                proceedings_df = pd.DataFrame({"event": pd.Series(dtype="str"),
                                               "name_venue": pd.Series(dtype="str"),
                                               "publisher": pd.Series(dtype="str"),
                                               "id_venue": pd.Series(dtype="str"),
                                                "venue_type": pd.Series(dtype="str")

                })

                journal_df['name_venue'] = df_publications[df_publications['venue_type'] == "journal"]['publication_venue'].astype('str')
                journal_df['publisher'] = df_publications[df_publications['venue_type'] == "journal"]['publisher'].astype('str')
                journal_df['id_venue'] = df_publications[df_publications['venue_type'] == "journal"]['id'].astype('str')
                journal_df['venue_type'] = df_publications[df_publications['venue_type'] == "journal"]['venue_type'].astype('str')
                journal_df.replace(to_replace="nan",value="")

                book_df['name_venue'] = df_publications[df_publications['venue_type'] == "book"]['publication_venue'].astype('str')
                book_df['publisher'] = df_publications[df_publications['venue_type'] == "book"]['publisher'].astype('str')
                book_df['id_venue'] = df_publications[df_publications['venue_type'] == "book"]['id'].astype('str')
                book_df['venue_type'] = df_publications[df_publications['venue_type'] == "book"]['venue_type'].astype('str')
                book_df.replace(to_replace="nan",value="")

                proceedings_df['event'] = df_publications[df_publications['venue_type'] == "proceedings"]['event'].astype('str')
                proceedings_df['name_venue'] = df_publications[df_publications['venue_type'] == "proceedings"]['publication_venue'].astype('str')
                proceedings_df['publisher'] = df_publications[df_publications['venue_type'] == "proceedings"]['publisher'].astype('str')
                proceedings_df['id_venue'] = df_publications[df_publications['venue_type'] == "proceedings"]['id'].astype('str')
                proceedings_df['venue_type'] = df_publications[df_publications['venue_type'] == "proceedings"]['venue_type'].astype('str')
                proceedings_df.replace(to_replace="nan",value="")

                # =============== DATABASE CONNECTION ===============

                with sql3.connect(self.db_path) as rdb:
                    journal_article_df.to_sql("JournalArticleTable", rdb, if_exists="append", index=False)
                    book_chapter_df.to_sql("BookChapterTable", rdb, if_exists="append", index=False)
                    proceeding_paper_df.to_sql("ProceedingsPaperTable", rdb, if_exists="append", index=False)
                    journal_df.to_sql("JournalTable", rdb, if_exists="append", index=False)
                    book_df.to_sql("BookTable", rdb, if_exists="append", index=False)
                    proceedings_df.to_sql("ProceedingsTable", rdb, if_exists="append", index=False)
                    rdb.commit()
            
            # =============== JSON UPLOAD DATA ===============

            elif filepath.endswith(".json"):
                with open(filepath, "r", encoding="utf-8") as file:
                    jsondata = json.load(file)
                    
                    # =============== AUTHORS DATAFRAME ===============
                    authors_df = pd.DataFrame({
                        "doi_authors": pd.Series(dtype="str"),
                        "family": pd.Series(dtype="str"),
                        "given": pd.Series(dtype="str"),
                        "orcid": pd.Series(dtype="str")
                    })

                    family = []
                    given = []
                    orcid = []
                    doi_authors = []

                    authors = jsondata['authors']
                    for key in authors:
                        for value in authors[key]:
                            doi_authors.append(key)
                            family.append(value['family'])
                            given.append(value['given'])
                            orcid.append(value['orcid'])

                    authors_df['doi_authors'] = doi_authors
                    authors_df['family'] = family
                    authors_df['given'] = given
                    authors_df['orcid'] = orcid

                    # =============== VENUES DATAFRAME ===============

                    venues_id_df = pd.DataFrame({
                        "doi_venues_id": pd.Series(dtype="str"),
                        "issn_isbn": pd.Series(dtype="str"),
                    })

                    doi_venues_id = []
                    issn_isbn = []

                    venues_id = jsondata["venues_id"]
                    for key in venues_id:
                        for value in venues_id[key]:
                            doi_venues_id.append(key)
                            issn_isbn.append(value)

                    venues_id_df["doi_venues_id"] = doi_venues_id
                    venues_id_df["issn_isbn"] = pd.Series(issn_isbn)

                    # =============== REFERENCES DATAFRAME ===============

                    references_df = pd.DataFrame({
                        "og_doi": pd.Series(dtype="str"),
                        "ref_doi": pd.Series(dtype="str"),
                    })

                    og_doi = []
                    ref_doi = []

                    references = jsondata["references"]
                    for key in references:
                        for value in references[key]:
                            og_doi.append(key)
                            ref_doi.append(value)

                    references_df["og_doi"] = pd.Series(og_doi)
                    references_df["ref_doi"] = pd.Series(ref_doi)

                    # =============== PUBLISHER DATAFRAME ===============

                    publishers_df = pd.DataFrame({
                        "crossref": pd.Series(dtype="str"),
                        "id_crossref": pd.Series(dtype="str"),
                        "name_pub": pd.Series(dtype="str")
                    })

                    crossref = []
                    id_crossref = []
                    name_pub = []

                    publishers = jsondata["publishers"]
                    for key in publishers:
                        crossref.append(key)
                        id_crossref.append(publishers[key]["id"])
                        name_pub.append(publishers[key]["name"])

                    publishers_df["crossref"] = pd.Series(crossref)
                    publishers_df["id_crossref"] = pd.Series(id_crossref)
                    publishers_df["name_pub"] = pd.Series(name_pub)
                
                # =============== DATABASE CONNECTION ===============

                with sql3.connect(self.db_path) as rdb:
                    authors_df.to_sql("AuthorsTable", rdb, if_exists="append", index=False)
                    venues_id_df.to_sql("VenuesIdTable", rdb, if_exists="append", index=False)
                    references_df.to_sql("ReferencesTable", rdb, if_exists="append", index=False)
                    publishers_df.to_sql("PublishersTable", rdb, if_exists="append", index=False)
                    rdb.commit()
            return True

# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––



# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

class RelationalQueryProcessor(QueryProcessor,RelationalProcessor):
    def __init__(self):
        super().__init__()

    def getPublicationsPublishedInYear(self, year):
        QR_1 = pd.DataFrame()
        print("don the things here")
        return QR_1

    def getPublicationsByAuthorId(self, orcid):
        QR_2 = pd.DataFrame()
        print("don the things here")
        return QR_2
    
    def getMostCitedPublication(self):
        QR_3 = pd.DataFrame()
        print("don the things here")
        return QR_3
    
    def getMostCitedVenue(self):
        QR_4 = pd.DataFrame()
        print("don the things here")
        return QR_4
    
    def getVenuesByPublisherId(self, crossref):
        QR_5 = pd.DataFrame()
        print("don the things here")
        return QR_5
    
    def getPublicationInVenue(self, issn_isbn):
        QR_6 = pd.DataFrame()
        print("don the things here")
        return QR_6
    
    def getJournalArticlesInIssue(self, issue, volume, jo_id):
        QR_7 = pd.DataFrame()
        print("don the things here")
        return QR_7
    
    def getJournalArticlesInVolume(self, volume, jo_id):
        QR_8 = pd.DataFrame()
        print("don the things here")
        return QR_8
    
    def getJournalArticlesInJournal(self, jo_id):
        QR_9 = pd.DataFrame()
        print("don the things here")
        return QR_9
    
    def getProceedingsByEvent(self, eventName):
        QR_10 = pd.DataFrame()
        print("don the things here")
        return QR_10
    
    def getPublicationAuthors(self, doi):
        QR_11 = pd.DataFrame()
        print("don the things here")
        return QR_11
    
    def getPublicationsByAuthorName(self, authorName):
        QR_12 = pd.DataFrame()
        print("don the things here")
        return QR_12
    
    def getDistinctPublisherOfPublications(self, doi_list):
        QR_13 = pd.DataFrame()
        print("don the things here")
        return QR_13
    
# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
#------------------------------------ANITA METHOD IS HERE-----------------------------#
#This method has been implemented by Anita Liviabella, following the professor guidelines order to retake the exam. 
#The purpose of the method of the is_publication_in_db(self, pub_id) method in the RelationalQueryProcessor class is to take in input a string and return a boolean: True if the publication identified by the input id is included in the dababase, False otherwise.

    def is_publication_in_db(self, pub_id):
        if type(pub_id) == str:
            with sql3.connect(self.getDbpath()) as qrdb:

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

# TEST AREA
rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbpath(rel_path)
rel_dp.uploadData("testData/relational_publications.csv")
rel_dp.uploadData("testData/relational_other_data.json")

# Checking the superclass is correct or not
print(rel_dp.__class__)

rel_qp = RelationalQueryProcessor()
rel_qp.setDbpath(rel_path)

# Checking the superclass is correct or not
print(rel_qp.__class__)
query_anita = rel_qp.is_publication_in_db("doi:10.1162/qss_a_00112")
print("is_publication_in_db Query\n", query_anita)