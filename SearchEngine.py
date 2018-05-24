import json
from collections import defaultdict
from pymongo import MongoClient
from math import log
from timeit import default_timer as timer

from bs4 import BeautifulSoup


# A generator that yields individual tokens, including tokens that can be empty ('')
def iterTokens(string):
    val = []
    for character in string:
        if character.isalnum():
            val.append(character)
        else:
            yield "".join(val)
            del val[:]
    if len(val) > 0:
        yield "".join(val)


# A generator that yields actual terms (by using the token generator but discarding all the empty tokens)
def iterTerms(string):
    for token in iterTokens(string):
        if token and not token.isdigit():
            yield token.lower()


# Helper function that calculates the term frequency
def getTermFrequency(numberOfThisWord, totalWordsInDoc):
    return float(numberOfThisWord / totalWordsInDoc)


# Helper function that calculates the inverse document frquency
def getInverseDocumentFrequency(documentsWithWord, totalDocuments):
    return float(log(totalDocuments / documentsWithWord))


# Function that loads up a document and terms it into terms and counts
# Returns a tuple: (dictionary whose key is a term and value is the terms count in this document, the number of terms in this document)
def getTermFrequenciesAndLength(identifier, url):
    totalTerms = 0
    terms = defaultdict(lambda: 0)
    # First we try to load the document as an HTML file
    try:
        soup = None

        with open("WEBPAGES_RAW/" + identifier) as fp:
            soup = BeautifulSoup(fp, "lxml")

        # If the document is an HTML document then iterate all the terms and increase their count in the dictionary
        for string in soup.stripped_strings:
            for term in iterTerms(string):
                terms[term] = terms[term] + 1
                totalTerms += 1
    except:  # If there is exception then the document is not HTML and we try to load it as plain text
        totalTerms = 0
        terms.clear()
        content = None

        try:
            with open("WEBPAGES_RAW/" + identifier, encoding='utf-8', errors='ignore') as fp:
                content = fp.readlines()
        except UnicodeDecodeError:  # Sometimes it can fail when loading strange file so print out a message
            print('Failed to decode url (id: {!s}): {!s}'.format(identifier, url))
            return ({}, 0)

        # If we can load it as plain text then iterate all the terms and increase their term counts in the dictionary
        for line in content:
            for term in iterTerms(line):
                terms[term] = terms[term] + 1
                totalTerms += 1

    # Return a non-defaultdict dictionary with the terms and counts and also return the number of terms in the document
    return (dict(terms), totalTerms)


# Takes a database connection, a search query, and the total number of documents in the corpus
# Returns a sorted list of (ID's, score) for the documents matching the query
def searchForQuery(posts, query, totalDocumentsCount) -> [str]:
    # Local function to get the weighted term frequency for the terms in the query (We aren't doing weighting though)
    def WTF(t, q):
        if getTermFrequency(t, q) == 0:
            return 0
        return 1 + log(getTermFrequency(t, q))

    # Eliminate repeating words so that the words list is a list of unique words (unordered)
    words = list(set(query.strip().lower().split(' ')))
    postingsList = []  # Postingslist and Postingsdict are the same stuff but used for iteration/lookup
    postingsDict = {}
    queryTfIdfs = {}
    # Go through all the words to get their posting list and calculate tf, idf, and tf-idf
    for index, word in enumerate(words):
        doc = posts.find_one({"term": word})
        # If no documents contain this term then remove it from the words list and search for docs with the other terms
        if doc is None:
            words.pop(index)
            continue
        p = doc['postings']
        idf = getInverseDocumentFrequency(len(p), totalDocumentsCount)  # idf is per document so we calc it once
        postingsList.append((word, p))
        postingsDict[word] = (p, idf)
        queryTfIdfs[word] = WTF(1, len(words)) * idf  # Calculate the tf-ids for this word in the query

    # Go through all the posting lists and eliminate postings for documents that don't have the entire query in them
    for word in words:
        # Go through each posting list
        masterList = postingsDict[word][0]

        def isInMasterList(docID):
            for tup in masterList:
                if tup[0] == docID:
                    return True
            return False

        for term, postingList in postingsList:
            if term != word:  # Don't care about the same word
                # Go through all the docIDs in this posting list
                for i in range(len(postingList) - 1, -1, -1):
                    docID, termCount, documentLength = postingList[i]
                    # If other documents from other terms don't have this word in them, then we don't care about them
                    if not isInMasterList(docID):
                        postingList.pop(i)
    # Now we are going to compute similarity scores
    scores = {}
    docLengths = {}
    for word in words:
        posting, idf = postingsDict[word]
        # For each document in the posting list for this word we are going to increments the scores
        for docID, termCount, documentLength in posting:
            tfidf = idf * getTermFrequency(termCount, documentLength)
            if docID in scores:
                scores[docID] = scores[docID] + (queryTfIdfs[word] * tfidf)
            else:
                scores[docID] = (queryTfIdfs[word] * tfidf)
            docLengths[docID] = documentLength

    # Normalize all of the scores using the length of each document
    for docID in scores.keys():
        scores[docID] = scores[docID] / docLengths[docID]

    # Return the final sorted list of document IDs and their scores
    ids = sorted([(key, value) for key, value in scores.items()], key=lambda x: -1 * x[1])
    return ids


# Executes when this module is run
if __name__ == '__main__':
    data = None
    try:  # Load the bookkeeping file
        with open('WEBPAGES_RAW/bookkeeping.json') as bookkeeping:
            data = json.load(bookkeeping)
    except:
        print('Failed to read the bookkeeping file! Shutting down.')
        exit(1)

    # total documents is the number of entries in the corpus
    totalDocuments = len(data)
    client = None
    db = None
    posts = None
    try:  # Connect to the mongo database
        client = MongoClient('localhost', 27017)
        db = client['test-index2']
        posts = db['term-collection2']
    except:
        print('Failed to establish a connection to MongoDb. Shutting down.')
        exit(1)

    # Get whether they want to index or search
    val = ""
    while not (val.strip().lower() == 'i' or val.strip().lower() == 's'):
        val = input('Do you want to index or search? (i/s)\n').strip().lower()

    # If they want to index then run the index creation routine
    if val == 'i':
        print('Found {!s} documents to index\n'.format(len(data)))
        # See if they want to reset the inverted index in the database
        val = input('Do you want to clear the index first? (yes-i-want-to/n)\n')
        if val.lower() in ['yes-i-want-to']:
            posts.drop()
            posts.create_index("term")

        # Ask for extra confirmation so they don't accidentally index documents twice
        val = input('Do you really want to mess up your index by re-indexing? (type yes-i-do):\n')
        if val == 'yes-i-do':
            startIndex = 0
            endIndex = 0
            while True:  # Get the indices of the documents they want to index (leaving it blank does all)
                val = input('Enter the document indices to index: (start:end)\n')
                if not val:  # If they leave it blank then index everything
                    startIndex = 0
                    endIndex = len(data) - 1
                    break

                split = val.split(':')
                s = split[0]
                e = split[1]
                if not s.isdigit() or not e.isdigit():
                    print('Indices must be numbers\n')
                    continue
                startIndex = int(s)
                endIndex = int(e)

                if startIndex < 0 or endIndex >= len(data) or startIndex > endIndex:
                    print(
                        'Start index must be greater than zero and less than end index. End index must be less than {!s}\n'
                            .format(len(data)))
                    continue
                break

            # Set up the timer and other variables before indexing
            start = timer()
            index = -1
            totalTermsIndexed = 0
            postingCache = {}
            postingCacheSize = 200000


            # Local function to write the posting cache to the database and then clear the in-memory cache
            def saveCache():
                print('Beginning to flush the postings cache of {!s} terms to the database'.format(
                    postingCacheSize))
                for term, posting in postingCache.items():
                    document = posts.find_one({"term": term})
                    # The term is already in the database
                    if document is not None:
                        # List of lists where each list corresponds to a document
                        postings = document['postings']
                        # Add our new document
                        postings.extend(posting)
                        document['postings'] = postings

                        posts.replace_one({'term': term}, document)
                    else:  # The term is not in the database
                        post = {"term": term,
                                "postings": posting}
                        posts.insert_one(post)
                postingCache.clear()
                print('Finished flushing the postings cache of {!s} terms'.format(postingCacheSize))


            # Go through all the document IDs in the bookkeeping file
            for documentId in data:
                index += 1
                if index > endIndex:
                    break
                if index >= startIndex:  # If we are within the right indices then go ahead and start indexing
                    # Get the term counts dictionary and the length of this file
                    termCounts, documentLength = getTermFrequenciesAndLength(documentId, data[documentId])
                    for term, termCount in termCounts.items():  # Go through all the terms and put them in posting cache
                        totalTermsIndexed += 1
                        if term in postingCache:  # Append new data to the posting for this term
                            postingCache[term].append([documentId, termCount, documentLength])
                        else:  # Insert term into the posting cache
                            postingCache[term] = [[documentId, termCount, documentLength]]
                    if len(postingCache) > postingCacheSize:  # If the posting cache gets too big then save it
                        print('Saving cache at index: {!s}'.format(index))
                        saveCache()
            print('Saving final cache')
            saveCache()  # Write the cache to disk at the very end
            end = timer()
            print('We indexed {!s} documents with {!s} total terms in {!s} seconds\n'.format(
                endIndex - startIndex, totalTermsIndexed, end - start))

    # This is the part that takes their search query input
    totalTermsInDb = posts.count()
    NUMBER_OF_LINKS_TO_PRINT = 20
    while True:
        inputVal = input('Enter a query(quit-66 to quit):\n').lower().strip()
        if inputVal == 'quit-66':
            break
        ids = searchForQuery(posts, inputVal, totalDocuments)
        if ids is not None and len(ids) > 0:
            for index, (id, tfidf) in enumerate(ids):
                if index >= NUMBER_OF_LINKS_TO_PRINT:  # We only print out a certain number of the links
                    break
                print('{!s}:  {!s}'.format(index + 1, data[id]))
