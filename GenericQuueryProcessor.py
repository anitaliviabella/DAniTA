# ==== NEW GENERIC QUERY PROCESSOR (2/7) ====
import pandas as pd
import extraRelationalClasses as rel   
import graphData_Manager as grp
# import extraGraphClasses as gra
import ModelClasses as dm
from pubMemory_full import *
from extraRelationalClasses import RelationalQueryProcessor
from graphData_Manager import QueryProcessor
from graphData_Manager import TriplestoreQueryProcessor


class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = list()
    def cleanQueryProcessors(self):
        self.queryProcessor = [] #sets the list as empty.
        return True
    def addQueryProcessor(self,processor):
        pClass = type(processor)
        if issubclass (pClass,rel.QueryProcessor): #it checks if the type of the processor is a subclass of rel.queryProcessor.
            self.queryProcessor.append(processor)
            df_creator(processor) 
            return True
        else:
            return False

    def getPublicationsPublishedInYear(self,year):
        final_DF = pd.DataFrame() #initialization of an empty df
            
        for item in self.queryProcessor: #it iterates over the list of queryprocesor
            result_DF = item.getPublicationsPublishedInYear(year)#calls the method on each query processor obtaining a df with publications published in year.
            final_DF = pd.concat([final_DF,result_DF]) #concaatenates the results.
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)

        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]

#getPublicationsByAuthorId: It returns a list of Publication objects referring to all the publications that have been authored by the person having the identifier specified as input (e.g. "0000-0001-9857-1511").
    def getPublicationsByAuthorId(self,id):
        #print('HEllo author')
        # The id is going to be a orcid
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getPublicationsByAuthorId(id)
            final_DF = pd.concat([final_DF,result_DF])
        print("final df:", final_DF)
        
        result = list()

        ids = set()
        if not final_DF.empty:  # Check if final_DF is not empty
            for idx, row in final_DF.iterrows():
                ids.add(row["id"])
        for item in ids:
            result.append(creatPubobj(item))
        return result


        #list[Publication]


#getMostCitedPublication: It returns the Publication object that has received the most number of citations by other publications.
    def getMostCitedPublication(self):
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getMostCitedPublication()
            final_DF = pd.concat([final_DF,result_DF])
        
        max_cit = final_DF["num_citations"].loc[final_DF.index[0]]  # This is the highest value of the "num_citations"... FOR NOW
        max_doi = final_DF["ref_doi"].loc[final_DF.index[0]]
        tuple1 = tuple((max_doi,max_cit))
        max_list = list(tuple1)
        for idx,row in final_DF:
            cit = row["num_citations"]
            if cit > max:
                max = cit
            elif cit == max:
                tpl = tuple((row["ref_doi"],cit))
                max_list.append(tpl)
        
        # Now we have a list of tuples [("ref_doi","num_citations")]
        #Publication -> here we will NOT return just a single Publication (as asked by the UML), but a list[Publication] because there may be multiple venues that are all the most cited (they have the same number of citations and therefore are all at the top of the descending order of cited venues)
        result = list()

        for tpl in max_list:
            for item in tpl:
                result.append(creatPubobj(item[0]))
                
        return result
        #Publication -> here we will NOT return just a single Publication (as asked by the UML), but a list[Publication] because there may be multiple venues that are all the most cited (they have the same number of citations and therefore are all at the top of the descending order of cited venues)

        ''''''


    def getMostCitedVenue(self):
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getMostCitedVenue()
            final_DF = pd.concat([final_DF,result_DF])
        
        # CHANGE THE FINAL_DF IN SUCH A WAY THAT IT HAS ONLY THE MOST CITED VENUES ***OF ALL***

        result = list()
        for idx,row in final_DF.iterrows():
            # Now we need to save all the information needed to create an object of class Venue
            # ["ids"](=issn_isbn), "title"(of the Venue), ["ids"](=crossref), "name"(of the Organisation) 
            title = row["name_venue"] # -> "title"(of the Venue)
            ids_list = list() # -> ["ids"](=issn_isbn)
            for idx2,row2 in final_DF.iterrows():
                title2 = row2["name_venue"]
                if title2 == title:
                    # We are in the same Venue!
                    issn_isbn = row2["issn_isbn"]
                    ids_list.append(issn_isbn)
            crossref_list = list() # -> ["ids"](=crossref)
            for idx3,row3 in final_DF.iterrows():
                title3 = row3["name_venue"]
                if title3 == title:
                    crossref = row3["publisher"]
                    crossref_list.append(crossref)
            name_org = row["name_pub"]
            publ_obj = dm.Organisation(crossref_list,name_org)
            venue_obj = dm.Venue(ids_list,title,publ_obj)
            result.append(venue_obj)
        return result
        #Venue -> here we will NOT return just a single Venue (as asked by the UML), but a list[Venue] because there may be multiple venues that are all the most cited (they have the same number of citations and therefore are all at the top of the descending order of cited venues)
        
    def getVenuesByPublisherId(self,id):
        # The id is going to be a crossref
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getVenuesByPublisherId(id)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            title = row["name_venue"] # -> "title"(of the Venue)
            ids_list = list() # -> ["ids"](=issn_isbn)
            for idx2,row2 in final_DF.iterrows():
                title2 = row2["name_venue"]
                if title2 == title:
                    # We are in the same Venue!
                    issn_isbn = row2["issn_isbn"]
                    ids_list.append(issn_isbn)
            crossref_list = list() # -> ["ids"](=crossref)
            for idx3,row3 in final_DF.iterrows():
                title3 = row3["name_venue"]
                if title3 == title:
                    crossref = row3["publisher"]
                    crossref_list.append(crossref)
            name_org = row["name_pub"]
            publ_obj = dm.Organisation(crossref_list,name_org)
            venue_obj = dm.Venue(ids_list,title,publ_obj)
            result.append(venue_obj)
        return result
        #list[Venue]
        
    def getPublicationInVenue(self,venueId):
        # The id is going to be a issn_isbn
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getPublicationInVenue(venueId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list()

        ids = set()
        for idx,row in final_DF.iterrows():
            ids.add(row["id"])
            
        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]

    def getJournalArticlesInIssue(self,issue,volume,journalId):
        # The id is going to be a doi
        #list[JournalArticle]
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getJournalArticlesInIssue(issue,volume,journalId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)

        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatJAobj(item))
        return result
        #list[Publication]

    def getJournalArticlesInVolume(self,volume,journalId):
        # The id is going to be a doi
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getJournalArticlesInVolume(volume,journalId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)
        print (final_DF)
        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatJAobj(item))
        return result
        #list[JournalArticle]

    def getJournalArticlesInJournal(self,journalId):
        # The id is going to be a doi
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getJournalArticlesInVolume(journalId,journalId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)

        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatJAobj(item))
        return result
        #list[JournalArticle]
        # ^^ currently not working!!

    def getProceedingsByEvent(self,eventPartialName):
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getProceedingsByEvent(eventPartialName)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            event = row["event"] # -> additional info "event" str
            title = row["name_venue"] # -> "title"(of the Venue)
            ids_list = list() # -> ["ids"](=issn_isbn)
            for idx2,row2 in final_DF.iterrows():
                title2 = row2["name_venue"]
                if title2 == title:
                    # We are in the same Venue!
                    issn_isbn = row2["issn_isbn"]
                    ids_list.append(issn_isbn)
            crossref_list = list() # -> ["ids"](=crossref)
            for idx3,row3 in final_DF.iterrows():
                title3 = row3["name_venue"]
                if title3 == title:
                    crossref = row3["publisher"]
                    crossref_list.append(crossref)
            name_org = row["name_pub"]
            publ_obj = dm.Organisation(crossref_list,name_org)
            proc_obj = dm.Proceedings(ids_list,title,publ_obj,event)
            result.append(proc_obj)
        return result
        #list[Proceeding]
        
    def getPublicationAuthors(self,publicationId):
        # The id is going to be a doi
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getPublicationAuthors(publicationId)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            #["ids"](=orcid), "givenName", "familyName"
            doi = row["doi_authors"]
            ids_list = list() # -> ["ids"](=orcid)
            for idx2,row2 in final_DF.iterrows():
                doi2 = row2["doi_authors"]
                if doi2 == doi:
                    # We are in the same publication
                    id = row2["orcid"]
                    ids_list.append(id)
            given = row["given"] # -> "givenName"
            family = row["family"] # -> "familyName"
            pers_obj = dm.Person(ids_list,given,family)
            result.append(pers_obj)
        return result
        #list[Person]

    def getPublicationsByAuthorName(self,authorPartialName):
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getPublicationsByAuthorName(authorPartialName)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list()

        ids = set()
        for idx,row in final_DF.iterrows():
            ids.add(row["id"])
            
        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]

    def getDistinctPublishersOfPublications(self,pubIdList):
        # The ids are going to be dois
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getDistinctPublishersOfPublications(pubIdList)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            #["ids"](=crossref), "name"(of the Organisation)
            doi = row["id_venue"]
            ids_list = list() # -> ["ids"](=crossref)
            for idx2,row2 in final_DF.iterrows():
                doi2 = row2["id_venue"]
                if doi2 == doi:
                    # We are in the same publication
                    id = row2["crossref"]
                    ids_list.append(id)
            name = row["name_pub"] # -> "name"(of the Organisation)
            pub_obj = dm.Organisation(ids_list,name)
            result.append(pub_obj)
        return result
        #list[Organisation]
    

#--------------------------ANITA----------------------
    def compute_h_index(self, author_id):
        publications = self.getPublicationsByAuthorId(author_id)
        citations = []
        #print("publications:", publications)

        # Collect citations for each publication
        for publication in publications:
            citations.append(self.get_citations(publication.id))  # get_citations is a method to get citations of a publication

        citations.sort(reverse=True)
        h_index = 0
        for i, citation in enumerate(citations):
            if citation >= i + 1:
                h_index += 1
            else:
                break

        return h_index

    def get_citations(self, publication_id):
        # Determine the type of publication_id and call the appropriate query method
        if publication_id.startswith("doi:"):
            for processor in self.queryProcessor:
                if isinstance(processor, RelationalQueryProcessor):
                    return processor.count_citations(publication_id)
                elif isinstance(processor, TriplestoreQueryProcessor):
                    return processor.count_citations(publication_id)
        else:
            # Handle other types of identifiers if necessary
            pass



    #It takes in input two different list of publications, and returns a new list that contains the union of the publication in both list (removing the duplicates).'''
    def remove_duplicates(self, l1, l2):
        #DOMANDA: non capisco se essendo questo metodo all'interno della classe query processor io debba elaborare una query che ritorni due liste di pubblicazioni!)
        combined_list = l1 + l2  # Combine both lists
        seen_ids = set()
        for publication in combined_list:
            seen_ids.add(publication)
        
        list_seen_ids = list(seen_ids)
        return list_seen_ids
    

### PERONI TESt
# print('hello 1')
# # Once all the classes are imported, first create the relational
# # database using the related source data
# rel_path = "relational.db"
# rel_dp = rel.RelationalDataProcessor()
# rel_dp.setDbPath(rel_path)
# rel_dp.uploadData("testData/relational_publications.csv")
# rel_dp.uploadData("testData/relational_other_data.json")

# # In the next passage, create the query processors for both
# # the databases, using the related classes
# rel_qp = rel.RelationalQueryProcessor()
# rel_qp.setDbPath(rel_path)


# # Finally, create a generic query processor for asking
# # about data
# generic = GenericQueryProcessor()
# generic.addQueryProcessor(rel_qp)


#METHOD TO TEST!
# h_index = generic.compute_h_index("0000-0001-5506-523X")
# print("H-index for the author:", h_index)
# print('hello 2')

# #1. c'è un problema nella query getPublicationsByAuthorId all'interno del metodo per l'h-index.

# remove_duplicates_method = generic.remove_duplicates(['doi:10.1162/qss_a_00023', 'doi:10.1038/sdata.2016.18'], ['doi:10.1007/s11192-020-03397-6', 'doi:10.1080/19386389.2021.1999156', 'doi:10.1038/sdata.2016.18'])
# print(remove_duplicates_method)
        

# #se creo le liste di dois da sola, il metodo di per se funziona appunto escludendo i duplicati.
