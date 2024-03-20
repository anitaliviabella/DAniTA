


'''
# GENERIC QUERY PROCESSOR: QUERIES AND METHODS
q1 = generic.getPublicationsPublishedInYear(2020)
#print("getPublicationsPublishedInYear Query\n",q1)

print("Methods for the objects of class Publication:\n")
for item in q1:
    print("ITEM")

    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())


q2 = generic.getPublicationsByAuthorId("0000-0001-9857-1511")
#print("getPublicationsByAuthorId Query\n",q2)


print("Methods for the objects of class Publication:\n")
for item in q2:
    print("ITEM")
    
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q3 = generic.getMostCitedPublication()
#print("getMostCitedPublication Query\n",q3)

print("Methods for the objects of class Publication:\n")
for item in q3:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q4 = generic.getMostCitedVenue()
print("getMostCitedVenue Query\n",q4)

print("Methods for the objects of class Venue:\n")
for item in q4:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getTitle()\n",item.getTitle())
    print("Method getPublisher()\n",item.getPublisher())

q5 = generic.getVenuesByPublisherId("crossref:78")
print("getVenuesByPublisherId Query\n",q5)

print("Methods for the objects of class Venue:\n")
for item in q5:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getTitle()\n",item.getTitle())
    print("Method getPublisher()\n",item.getPublisher())

q6 = generic.getPublicationInVenue("issn:0944-1344")
print("getPublicationInVenue Query\n",q6)

print("Methods for the objects of class Publication:\n")
for item in q6:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())


q7 = generic.getJournalArticlesInIssue("9","17","issn:2164-5515")
print("getJournalArticleInIssue Query\n",q7)

print("Methods for the objects of class Publication:\n")
for item in q7:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())


q8 = generic.getJournalArticlesInVolume("17","issn:2164-5515")
print("getJournalArticleInVolume Query\n",q8)

print("Methods for the objects of class Publication:\n")
for item in q8:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q9 = generic.getJournalArticlesInJournal("issn:2164-5515")
print("getJournalArticleInJournal Query\n",q9)

print("Methods for the objects of class Publication:\n")
for item in q9:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q10 = generic.getProceedingsByEvent("web")
print("getProceedingsByEvent Query\n",q10)

print("Methods for the objects of class Proceedings:\n")
for item in q10:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getTitle()\n",item.getTitle())
    print("Method getPublisher()\n",item.getPublisher())
    print("Method getEvent()\n",item.getEvent())

q11 = generic.getPublicationAuthors("doi:10.1080/21645515.2021.1910000")
print("getPublicationAuthors Query\n",q11)

print("Methods for the objects of class Person:\n")
for item in q11:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getGivenName()\n",item.getGivenName())
    print("Method getFamilyName()\n",item.getFamilyName())

q12 = generic.getPublicationsByAuthorName("sil")
print("getPublicationsByAuthorName Query\n",q12)

print("Methods for the objects of class Publication:\n")
for item in q12:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q13 = generic.getDistinctPublishersOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"])
print("getDistinctPublisherOfPublications Query\n",q13)

print("Methods for the objects of class Organisation:\n")
for item in q13:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getName()\n",item.getName())


#getPublicationsByAuthorId: It returns a data frame with all the publications (i.e. the rows) that have been authored by the person having the identifier specified as input (e.g. "0000-0001-9857-1511").
    def getPublicationsByAuthorId(self,id):
        if type(id) == str:
            print("ehi")
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                # 2 === Also here I am missing the information regarded the cited Publications (as it does not make any sense to include it here), of which I only save the doi... therefore, I guess that in the generic we will have to retrieve all that information from the database (open a new connection?)
                query = """
                    SELECT 
                        doi_authors, family, given, orcid, title, id
                    FROM 
                        AuthorsTable
                        LEFT JOIN JournalArticleTable ON AuthorsTable.doi_authors == JournalArticleTable.id
                    WHERE
                        orcid='{orcid}'""".format(orcid=id)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            print(pd.DataFrame(data=result,columns=["family", "given", "orcid", "doi_authors", "title", "id"]))
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "pub_type", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub", "venue_type"])
        else:
            raiseExceptions("The input parameter is not a string!")
'''