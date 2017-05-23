#!/usr/bin/env python
#sum the products of corresponding coordinates, sort results in descending order

#tags account for by dividing the tfidf into fractions
#proprotional to the number of word's occurences in either tag or untagged
#summing them up with the weights accounted for 
import os
import re
import sys
import json
from math import log, sqrt
from collections import defaultdict

from nltk.stem.snowball import SnowballStemmer

from indexer import tokenize

data_folder = 'WEBPAGES_SIMPLE'
index_fname = 'index.json'
links_fname = 'bookkeeping.tsv'

# weights for words in different tags, including no tag
tag_weights = {'h1':0.9, 'h2':0.8, 'h3':0.7, 'b':0.6, 'none':0.5}

# compile regular expression for removing whitespace from snippets  
redundant_whitespace_re = re.compile(r'\s+', flags=re.MULTILINE)

# set this to an integer n to show only n first links found
search_cutoff = None

def load_links():
    """
    Make dict of filenames to links
    and links to filenames of the indexed data.
    """
    links_dict = {}
    with open(os.path.join(data_folder, links_fname), 'r') as links_file:
        for line in links_file:
            fname, url = line.strip().split('\t')
            links_dict[fname] = url
    return links_dict

def relevancy(doc_vector, search_vector):
    """
    Get relevancy of the doc_vector against the search_vector.
    """
    return sum(i * j for i, j in zip(doc_vector, search_vector))

def account_for_tags(tfidf, tags):
    """
    Modify tfidf to account for the tags.
    """
    total_count = float(sum(tags.values()))
    return tfidf * sum(tag_weights[tag] * tags[tag] for tag in tags) / total_count

def get_snippets(doc, search_terms, terms_idf_vector, window=37):
    """
    Get a snippet of the document containing search terms.
    """
    snippets = []
    # get stems
    stems = []
    for search_term in search_terms:
        stem = stemmer.stem(search_term)
        if stem != search_term:
            stems.append(stem)
    # compile regular expression to find all search terms in the text
    search_re_line = r'\b{}\b'.format(r'\b|\b'.join(search_terms))
    if stems != []:
        search_re_line += r'|' + r'|'.join(stems)
    search_term_re = re.compile(search_re_line, flags=re.IGNORECASE)
    with open(os.path.join(data_folder, doc), 'r') as docfile:
        # read the text fro file removing redundant whitespace at the same time
        text = redundant_whitespace_re.sub(' ', docfile.read())
    # look for all occurences of search terms
    for match in search_term_re.finditer(text):
        # for each, add a piece of text within (start-of-search-term - window, start-of-search-term + window) range
        snippets.append('...{}...'.format(text[max(0, match.start(0)-window):min(len(text), match.start(0)+window)].strip()))
    return snippets

if __name__ == "__main__":
    # load index
    with open(index_fname, 'r') as index_file:
        index = json.load(index_file)

    # load files-to-links info from bookkeping.csv
    links_dict = load_links()

    # initialize stemmer
    stemmer = SnowballStemmer('english')

    # obtain the search line and tokenize it to search terms
    search_line = raw_input('Search: ')
    search_terms = tokenize(search_line)

    # collect the documents containing any of the search terms
    # and the IDFs for the search terms

    # a dict where keys are documents, and values are vectors of
    # TFIDFs of search terms contained in the document 
    doc_dict = dict()

    # IDFs of the search terms that have any occurences in the index
    terms_idf_vector = []

    # terms which actually have any occurences in the index  
    actual_terms = []
    for item_position, search_term in enumerate(search_terms):
        # check each search term
        stem = stemmer.stem(search_term)
        if search_term in index or 's.' + stem in index:
            # search term occurs in index
            terms_idf_vector.append(index[search_term][0])
            actual_terms.append(search_term)

            occurences = index[search_term][1]

            # if stem is different from the word form, search for it too
            if stem != search_term:
                occurences.extend(index['s.' + stem][1])

            # go through all documents containing the term
            for doc, tfidf, tags in occurences:
                if doc not in doc_dict:
                    # if no of the previously checked search terms were
                    # in this document, add a new vector with zeros for those terms
                    doc_dict[doc] = [0 for i in xrange(item_position)]
                doc_dict[doc].append(account_for_tags(tfidf, tags))

            # check all documents in doc_dict and add zeros for the term's position
            # if we didn't already add its TFIDF (meaning this term is not in this document)
            for doc in doc_dict:
                if len(doc_dict[doc]) <= item_position:
                    doc_dict[doc].append(0)        

    # first, separate results into how many terms from search string were found
    terms_count = len(actual_terms)
    bucketed_doc_dict = [{} for i in range(terms_count)]
    for doc in doc_dict:
        terms_lacking = doc_dict[doc].count(0)
        bucketed_doc_dict[terms_lacking][doc] = doc_dict[doc]

    # horizontal lines to separate blocks of output
    head_hr = '=' * 80
    tail_hr = '=' * 80
    mid_hr = '\n' + '-' * 80 + '\n'
    i = 1
    for doc_bucket in bucketed_doc_dict:
        for doc in sorted(doc_bucket, key=lambda x: relevancy(doc_bucket[x], terms_idf_vector), reverse=True):
            print head_hr
            # link
            print i, links_dict[doc]
            # local file
            print 'cached copy:', doc
            print tail_hr
            # snippets containing search terms
            print mid_hr.join([''] + get_snippets(doc, actual_terms, terms_idf_vector) + [''])
            i += 1
            if search_cutoff is not None and i > search_cutoff:
                sys.exit(0)
