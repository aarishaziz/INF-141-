#!/usr/bin/env python

import os
import re
import json
from math import log
from collections import defaultdict

from nltk.stem.snowball import SnowballStemmer

data_folder = 'WEBPAGES_SIMPLE'
index_fname = 'index.json'

tag_list = ('h1', 'h2', 'h3', 'b')

tag_re = re.compile(r'^<(/?)(.*?)>$')

# line containing punctuation symbols to strip them off
punctuation = '!"#$%&\'()*+,-./:;=?@[\\]^_`{|}~'

def get_flist(folder):
    """
    Get paths to all files in subfolders of folder.
    """
    flist = []
    for candname in sorted(os.listdir(folder)):
        candpath = os.path.join(folder, candname)
        if os.path.isdir(candpath): # add all files in folder
            for fname in sorted(os.listdir(candpath)):
                flist.append(os.path.join(candname, fname))
    return flist

def tokenize(line):
    """
    Tokenize line.
    """
    line = line.strip()

    line = line.replace('--', ' -- ')

    line = line.replace('<strong>', '<b>').replace('</strong>', '</b>')

    line = line.replace('<', ' <').replace('>', '> ')

    tokens = line.split()

    tokens = [token.strip(punctuation).lower() for token in tokens]

    tokens = [token for token in tokens if token != ""]
    return tokens

def parse_tag(token):
    """
    Check if token is an HTML tag.
    Return tag, and '/' if it's a closing one.
    """
    match = tag_re.match(token)
    if match is not None:
        return match.group(2), match.group(1)

    return None, None

def index_token(token, tag_list, tags_dict, counter_dict):
    """
    Add token with info about its tags
    to local index for one document.
    """
    counter_dict.setdefault(token, [0, defaultdict(int)])
    counter_dict[token][0] += 1

    for tag in tag_list:
        if tags_dict[tag]:
            counter_dict[token][1][tag] += 1
        else:
            counter_dict[token][1]['none'] += 1
    return counter_dict

def create_index(root_folder, flist, index_fname):
    """
    Create inverse index.
    """
    # initialize stemmer
    stemmer = SnowballStemmer('english')

    index_dict = defaultdict(list)
    total_word_count = 0
    for path in flist:
        # dict to keep track of all open tags 
        tags_dict = {tag: False for tag in tag_list}

        print 'Indexing', path

        # dict to keep track of all tags' occurences
        counter_dict = dict()
        doc_word_count = 0

        with open(os.path.join(root_folder, path), 'r') as infile:
            for line in infile:
                for token in tokenize(line):
                    tag, closing = parse_tag(token)
                    if tag is not None: 
                        if tag in tags_dict:
                            # update tags_dict
                            tags_dict[tag] = (closing != '/')
                    else: 
                        counter_dict = index_token(token, tag_list, tags_dict, counter_dict)
                        doc_word_count += 1
                        total_word_count += 1

                        try:
                            stem = stemmer.stem(token)

                            # check if the stem is different from the word form
                            if stem != token:
                                counter_dict = index_token('s.' + stem, tag_list, tags_dict, counter_dict)
                        except UnicodeDecodeError:
                            pass
                        
        # convert token counts to TF
        for token in counter_dict:
            index_dict[token].append((path, float(counter_dict[token][0]) / float(doc_word_count), dict(counter_dict[token][1])))

    # convert TF to TFIDF
    total_docs = float(len(flist))
    for token in index_dict:
        total_docs_with_token = float(len(index_dict[token]))
        idf = log(total_docs / total_docs_with_token)
        index_dict[token] = [idf, [(path, tf * idf, tags) for path, tf, tags in index_dict[token]]]

    # output index to json
    with open(index_fname, 'w') as index_json_file:
        json.dump(index_dict, index_json_file)

    # output statistics
    print 'Total documents: {}'.format(len(flist))
    print 'Total words: {}'.format(total_word_count)
    print 'Total unique words: {}'.format(len([word for word in index_dict if not word.startswith('s.')]))
    print 'Index size: {} Kb'.format(os.path.getsize(index_fname) / 1024)

if __name__ == "__main__":
    flist = get_flist(data_folder)
    create_index(data_folder, flist, index_fname)
