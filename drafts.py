''' def compute_h_index(self, author_id):
        if not isinstance(author_id, str):
            raise ValueError("Author ID should be a string")

        # Combine results from all query processors
        all_publications = []
        for processor in self.queryProcessor:
            publications = processor.getPublicationsByAuthorId(author_id)
            all_publications.extend(publications)

        # Calculate H-index
        h_index = 0
        all_citations = [publication['citation_count'] for publication in all_publications]
        all_citations.sort(reverse=True)

        for i, citations in enumerate(all_citations):
            if i >= citations:
                h_index = i
                break

        return h_index



    def compute_h_index(self, orcid):
        if not isinstance(orcid, str):
            raise ValueError("Author ID should be a string")

    # Step 1: Get all publications by the author
        author_publications = self.getPublicationsByAuthorId(orcid)

    # Step 2: Create a list of citation counts for each publication
        citation_counts = [publication.get_citation_count() for publication in author_publications if publication is not None]

    # Step 3: Sort the citation counts in descending order
        sorted_citations = sorted(citation_counts, reverse=True)

    # Step 4: Find the h-index
        h_index = 0
        for h, citations in enumerate(sorted_citations, start=1):
            if h <= citations:
                h_index = h
            else:
                break

        return h_index
        
        
    def compute_h_index(self, author_id: str) -> int:
        if not isinstance(author_id, str):
            raise ValueError("Author ID must be a string.")
        
        max_h_index = 0
        for processor in self.queryProcessor:
            most_cited_publications = processor.getMostCitedPublication()
            author_publications = most_cited_publications[most_cited_publications['author_id'] == author_id]
            citations = author_publications['num_citations'].sort_values(ascending=False)
            h_index = sum(1 for c in citations if c >= max_h_index + 1)
            max_h_index = max(max_h_index, h_index)
        return max_h_index


# Ensure that the author_id_for_test has some publications with citations
publications_for_test = generic.getPublicationsByAuthorId(author_id_for_test)
for publication in publications_for_test:
    # Let's add a few dummy citations for each publication
    for i in range(1, 4):
        dummy_citation = rel.Publication(f"dummy_doi_{i}")
        publication.addCitedPublication(dummy_citation)
# Print the list of publications and their citations
print("\nPublications for the specified author:")
for publication in publications_for_test:
    print(f"  - {publication.getTitle()} ({len(publication.getCitedPublications())} citations)")

# Run the compute_h_index method for the specified author_id
computed_h_index = generic.compute_h_index(author_id_for_test)

# Print the results
print(f"\nComputed H-Index for author {author_id_for_test}: {computed_h_index}\n")


    def compute_h_index(self, author_id):
        # Get all publications by the author
        publications_by_author = []
        for processor in self.queryProcessor:
            publications_by_author.extend(processor.getPublicationsByAuthorId(author_id))

        # Count citations for each publication
        citation_counts = [len(publication.getCitedPublications()) for publication in publications_by_author]

        # Sort citation counts in descending order
        citation_counts.sort(reverse=True)

        # Find h-index
        h_index = 0
        for i, citations in enumerate(citation_counts):
            if i + 1 <= citations:
                h_index = i + 1
            else:
                break

        return h_index

 def getPublicationCitations(self, doi):
        if isinstance(doi, str):
            endpoint = self.getEndpointUrl()
            query = """
            PREFIX schema: <https://schema.org/>

            SELECT (COUNT(?citation) AS ?num_citations)
            WHERE {{
                ?publication schema:identifier "{doi}" ;
                             schema:citation ?citation.
            }}
            """
            result = get(endpoint, query, True)
            if not result.empty:
                num_citations = int(result["num_citations"].iloc[0])
                return num_citations
            else:
                return 0  # No citations found
        else:
            raise ValueError("Publication DOI must be a string.")
            
            '''