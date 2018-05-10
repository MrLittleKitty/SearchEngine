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

    return {key: (value / totalTerms) for key, value in terms.items()}


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
        val = input('Do you want to clear the index first? (y/n)\n')
        if val.lower() in ['y', 'yes']:
            posts.drop()
            posts.create_index("term")

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
                        idf = log(totalDocuments / docsWithTerm)

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
    inputVal = ""
    while inputVal != 'fuckingquit':
        inputVal = input('Enter a term to query ({!s} possible):\n'.format(totalTermsInDb)).lower().strip()
        if " " in inputVal:
            print('No spaces allowed')
            continue
        document = posts.find_one({"term": inputVal})
        if document is None:
            print("Could not find the term '{!s}' in the index".format(inputVal))
            continue
        for index, posting in enumerate(document['postings']):
            print("{!s}: {!s} -- {!s} -- ({!s})".format(index + 1, data[posting[0]], posting[2], posting[0]))
