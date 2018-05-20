import json
from collections import defaultdict
from pymongo import MongoClient
from math import log
from timeit import default_timer as timer

from bs4 import BeautifulSoup


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


def iterTerms(string):
    for token in iterTokens(string):
        if token and not token.isdigit():
            yield token.lower()


def getTermFrequency(numberOfThisWord, totalWordsInDoc):
    return float(numberOfThisWord / totalWordsInDoc)


def getInverseDocumentFrequency(documentsWithWord, totalDocuments):
    return float(log(totalDocuments / documentsWithWord))


def getTermFrequencies(identifier, url):
    totalTerms = 0
    terms = defaultdict(lambda: 0)
    try:
        soup = None

        with open("WEBPAGES_RAW/" + identifier) as fp:
            soup = BeautifulSoup(fp, "lxml")

        for string in soup.stripped_strings:
            for term in iterTerms(string):
                terms[term] = terms[term] + 1
                totalTerms += 1
    except:
        totalTerms = 0
        terms.clear()
        content = None

        try:
            with open("WEBPAGES_RAW/" + identifier) as fp:
                content = fp.readlines()
        except UnicodeDecodeError:
            print('Failed to decode url (id: {!s}): {!s}'.format(identifier, url))
            return {}

        for line in content:
            for term in iterTerms(line):
                terms[term] = terms[term] + 1
                totalTerms += 1

    return {key: getTermFrequency(value, totalTerms) for key, value in terms.items()}


def searchForQuery(posts, query, totalDocumentsCount) -> [str]:
    def WTF(t, q):
        if getTermFrequency(t, q) == 0:
            return 0
        return 1 + log(getTermFrequency(t, q))

    words = list(set(query.strip().lower().split(' ')))
    postingsList = []
    postingsDict = {}
    queryTfIdfs = {}
    for index, word in enumerate(words):
        doc = posts.find_one({"term": word})
        if doc is None:
            words.pop(index)
            continue
        p = doc['postings']
        idf = getInverseDocumentFrequency(len(p), totalDocumentsCount)
        postingsList.append((word, p))
        postingsDict[word] = (p, idf)
        queryTfIdfs[word] = WTF(1, len(words)) * idf

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
                    docID, termFrequency, tfidf = postingList[i]
                    # If other documents from other terms don't have this word in them, then we don't care about them
                    if not isInMasterList(docID):
                        postingList.pop(i)
    scores = {}
    magnitudes = {}

    # Sort by idf and put high idf terms at the front
    # postings.sort(key=lambda posting: -1 * posting[1])


if __name__ == '__main__':
    data = None
    try:
        with open('WEBPAGES_RAW/bookkeeping.json') as bookkeeping:
            data = json.load(bookkeeping)
    except:
        print('Failed to read the bookkeeping file! Shutting down.')
        exit(1)

    totalDocuments = len(data)
    client = None
    db = None
    posts = None
    try:
        client = MongoClient('localhost', 27017)
        db = client['test-index']
        posts = db['term-collection']
    except:
        print('Failed to establish a connection to MongoDb. Shutting down.')
        exit(1)

    val = ""
    while not (val.strip().lower() == 'i' or val.strip().lower() == 's'):
        val = input('Do you want to index or search? (i/s)\n').strip().lower()

    if val == 'i':
        print('Found {!s} documents to index\n'.format(len(data)))
        val = input('Do you want to clear the index first? (yes-i-want-to/n)\n')
        if val.lower() in ['yes-i-want-to']:
            posts.drop()
            posts.create_index("term")
        val = input('Do you really want to mess up your index by re-indexing? (type yes-i-do):\n')
        if val == 'yes-i-do':
            startIndex = 0
            endIndex = 0
            while True:
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

            start = timer()
            index = -1
            totalTermsIndexed = 0
            newTermsIndexed = 0
            for documentId in data:
                index += 1
                if index > endIndex:
                    break

                if index >= startIndex:
                    termFrequencies = getTermFrequencies(documentId, data[documentId])
                    for term, termFrequency in termFrequencies.items():
                        totalTermsIndexed += 1
                        document = posts.find_one({"term": term})
                        # The term is already in the database
                        if document is not None:
                            # List of lists where each list corresponds to a document
                            postings = document['postings']
                            # Calculate the inverse document frequency (idf) for this term
                            docsWithTerm = len(postings) + 1
                            idf = getInverseDocumentFrequency(docsWithTerm, totalDocuments)

                            # Update the tf-idf score for all documents because we are adding a new document
                            for posting in postings:
                                posting[2] = posting[1] * idf
                            # Add our new document
                            postings.append([documentId, termFrequency, termFrequency * idf])
                            document['postings'] = postings

                            posts.replace_one({'term': term}, document)
                        else:  # The term is not in the database
                            newTermsIndexed += 1
                            # Inverse document frequency (idf) is pretty easy when there is only one document so far
                            idf = log(totalDocuments)
                            post = {"term": term,
                                    "postings": [[documentId, termFrequency, termFrequency * idf]]}
                            posts.insert_one(post)

            end = timer()
            print('We indexed {!s} documents with {!s} total terms and {!s} new terms in {!s} seconds\n'.format(
                endIndex - startIndex, totalTermsIndexed, newTermsIndexed, end - start))

    totalTermsInDb = posts.count()
    while True:
        inputVal = input('Enter a query(quit-66 to quit):\n').lower().strip()
        if inputVal == 'quit-66':
            break
        ids = searchForQuery(posts, inputVal, totalDocuments)
        if ids is not None and len(ids) > 0:
            for index, id in enumerate(ids):
                print('{!s}: {!s}'.format(index + 1, data[id]))
