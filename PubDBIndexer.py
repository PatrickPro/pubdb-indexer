# -*- coding: iso-8859-1 -*-
__author__ = 'patrickpro'

import os
import os.path
import codecs

from BeautifulSoup import BeautifulSoup
from  pymongo import MongoClient
import pymongo


PUB_PATH = '/Users/patrickpro/DEVELOPMENT/pubdb/pub'
PEOPLE_PATH = '/Users/patrickpro/DEVELOPMENT/pubdb/people'
client = MongoClient('localhost', 27017)


db = client['pubdb-dev']
pubCollection = db['publications']
authorCollection = db['authors']


def processADIT(file):
    if os.path.isfile(file):
        try:
            f = codecs.open(file, 'r', 'iso-8859-1')
            text = ''
            for line in f:
                if not line.startswith('#') and not line.startswith('\n') and not line.startswith('\r'):
                    if text is not '':
                        text += ' ' + line.rstrip()
                    else:
                        text = line.rstrip()
            return text
            f.close()
        except IOError as err:
            print "ERROR processADIT() - " + file
            print err
    else:
        print "File not found: " + file.replace(PUB_PATH, '')
        return ''


def processAuthorPlainText(file):
    try:
        f = codecs.open(file, 'r', 'iso-8859-1')
        authors = []
        for line in f:
            if not line.startswith('#') and not line.startswith('\n') and not line.startswith('\r'):
                af = codecs.open(PEOPLE_PATH + '/' + line.rstrip() + '.link', 'r', 'iso-8859-1')
                name = af.read().replace('\n', '').replace('\r', '')

                if name.startswith('<'):
                    # link
                    soup = BeautifulSoup(name)
                    name = soup.find('a').getText()
                    authors.append(name.encode('utf8', 'replace').replace(".", ""))
                else:
                    # name only
                    authors.append(name.encode('utf8', 'replace').replace(".", ""))
        f.close()
        af.close()
        return authors
    except IOError as err:
        print "ERROR while processing " + file.replace(PUB_PATH, '')
        print err
        return authors


def getAuthorIDs(authors):
    try:
        authorsID = []
        for author in authors:
            query = list(db.authors.find({'name': author}, {"_id": 1}))
            authID = str(query).replace("[{u'_id': u'", "").replace("'}]", "")
            authorsID.append(authID)
        return authorsID

    except IOError as err:
        print "ERROR getAuthorIDs() - " + list
        print err


def processKeywords(file):
    if os.path.isfile(file):
        try:
            f = codecs.open(file, 'r', 'iso-8859-1')
            keywords = []
            type = []
            awards = []
            for line in f:
                if not line.startswith('#') and not line.startswith('\n') and not line.startswith(
                        '\r') and not line.startswith('atSelectedPub'):

                    if '-publication' in line:
                        if 'conference-publication' in line:
                            type.append(line.replace("-publication", "").rstrip())
                        elif 'journal-publication' in line:
                            type.append(line.replace("-publication", "").rstrip())
                        elif 'technical-publication' in line:
                            type.append(line.replace("-publication", "").rstrip())
                        elif 'workshop-publication' in line:
                            type.append(line.replace("-publication", "").rstrip())
                        elif 'thesis-publication' in line:
                            type.append(line.replace("-publication", "").rstrip())
                        elif 'proceedings-book-publication' in line:
                            type.append(line.replace("-publication", "").rstrip())
                        elif 'web-feature' in line:
                            type.append(line.rstrip())
                            print 'web-feature ' + file
                        else:
                            print "Not a valid publication type! " + line.rstrip() + ' file: ' + file.replace(PUB_PATH,
                                                                                                              '') + 'using "unknown-publication"'
                            type.append('unknown')
                    elif '.award' in line:
                        award = line.rstrip().replace(".award", '') if ".award" in line.rstrip() else "none"
                        awards.append(award)
                    else:
                        # found finally some real keywords ;)
                        keywords.append(line.rstrip())
            f.close()

            if len(type) is 0:
                print 'No pub type set! - using "unknown-publication" as default value ' + file.replace(PUB_PATH, '')
                type.append('unknown-publication')

            returnKeyDic = {
                'type': type[0],
                'keywords': keywords,
                'awards': "".join(awards)
            }

            return returnKeyDic
        except IOError as err:
            print "ERROR processKeywords() - " + file.replace(PUB_PATH, '')
            print err
    else:
        print "File not found: " + file.replace(PUB_PATH, '')
        return {
            'type': '',
            'keywords': '',
            'awards': ''
        }


def migratePublications2Mongo():
    print("[INFO] Processing publications...")

    oldestPub = 9000
    newestPub = 0

    folder = os.listdir(PUB_PATH)
    for pubFolder in folder:
        if (pubFolder != ".svn" and pubFolder != ".DS_Store"):
            print "Processing: " + pubFolder

            abstractFile = PUB_PATH + "/" + pubFolder + "/" + pubFolder + ".abstract"
            authorsFile = PUB_PATH + "/" + pubFolder + "/" + pubFolder + ".authors"
            dateFile = PUB_PATH + "/" + pubFolder + "/" + pubFolder + ".date"
            infoFile = PUB_PATH + "/" + pubFolder + "/" + pubFolder + ".info"
            keywordsFile = PUB_PATH + "/" + pubFolder + "/" + pubFolder + ".keywords"
            pdfFile = PUB_PATH + "/" + pubFolder + "/" + pubFolder + ".pdf"
            titleFile = PUB_PATH + "/" + pubFolder + "/" + pubFolder + ".title"

            authors = processAuthorPlainText(authorsFile)
            keywordDic = processKeywords(keywordsFile)
            title = processADIT(titleFile)
            abstract = processADIT(abstractFile)
            info = processADIT(infoFile)
            date = processADIT(dateFile)

            if len(date.split("/")) > 3:
                print date + pubFolder
            year = int(date) if len(date.split("/")) == 1 else int(date.split("/")[0])
            month = 0 if len(date.split("/")) < 2 or len(date.split("/")[1]) < 1 else int(date.split("/")[1])
            day = 0 if len(date.split("/")) < 3 or len(date.split("/")[2][:2]) < 1  else int(
                date.split("/")[2][:2])  # fix for date ranges and stupid people, only use first date

            if year < oldestPub:
                oldestPub = year
            if year > newestPub:
                newestPub = year

            if os.path.isfile(pdfFile):
                pdfURL = 'http://www.medien.ifi.lmu.de/pubdb/publications/pub/' + pubFolder + '/' + pubFolder + '.pdf'
            else:
                pdfURL = ''

            pub = {
                "title": title,
                "abstact": abstract,
                "authors": authors,
                "date": date.replace("/", "-"),  # slash does not work in REST api
                "year": year,
                "month": month,
                "day": day,
                "type": keywordDic['type'],
                "keywords": keywordDic['keywords'],
                "info": info,
                "pdf": pdfURL,
                "filename": pubFolder,
            }
            if len(keywordDic['awards']) > 0:
                pub.update({"awards": keywordDic['awards']})
            pubCollection.insert(pub)


def migrateAuthors2Mongo():
    print("[INFO] Processing authors...")
    folder = os.listdir(PEOPLE_PATH)
    for peopleFolder in folder:
        if (peopleFolder != ".svn" and peopleFolder != ".DS_Store"):
            authorsFile = PEOPLE_PATH + '/' + peopleFolder

            name = ''
            url = ''
            try:
                f = codecs.open(authorsFile, 'r', 'iso-8859-1')
                line = f.read()
                if line.startswith('<'):
                    # link
                    soup = BeautifulSoup(line.rstrip())
                    name = soup.find('a').getText().encode('utf8', 'replace').replace(".", "")
                    url = soup.find('a').get('href')
                else:
                    # name only
                    name = line.rstrip().encode('utf8', 'replace').replace(".", "")
                    url = ''
                f.close()
            except (IOError):
                print "ERROR - migrateAuthors2Mongo()"

            try:
                author = {
                    "_id": name,
                    "url": url,
                    "publishedWith": {},
                    "count": {}
                }

                authorCollection.insert(author)
            except (pymongo.errors.DuplicateKeyError):
                print "Dublicate Author: " + name


def updateCPC():
    print "[INFO] Updating count, pubs and co-authors for each author"

    authors = list(db.authors.find({}, {"_id": 1}))  # get all authors from DB
    for aut in authors:
        # find min/max pubYear for this author
        authorYears = list(db.publications.find({"authors": aut["_id"]}, {"year": 1, "_id": 0}))
        minYear = 9000
        maxYear = 0
        # get all years, save lowest and highest in two variables (start and end)
        for yearObj in authorYears:
            year = yearObj["year"]
            if year < minYear:
                minYear = year
            if year > maxYear:
                maxYear = year


        # set the count for each publication type
        countDic = {}
        coauthDic = {}
        pubs = []
        for year in xrange(int(minYear), int(maxYear) + 1):
            print "Processing relations for: " + aut["_id"].encode('utf8', 'replace') + ", " + str(year)
            conference_count = int(
                db.publications.find({"authors": aut["_id"], "type": "conference", "year": int(year)},
                                     {"authors": 1}).count())
            journal_count = int(db.publications.find({"authors": aut["_id"], "type": "journal", "year": int(year)},
                                                     {"authors": 1}).count())
            technical_count = int(db.publications.find({"authors": aut["_id"], "type": "technical", "year": int(year)},
                                                       {"authors": 1}).count())
            workshop_count = int(db.publications.find({"authors": aut["_id"], "type": "workshop", "year": int(year)},
                                                      {"authors": 1}).count())
            thesis_count = int(db.publications.find({"authors": aut["_id"], "type": "thesis", "year": int(year)},
                                                    {"authors": 1}).count())
            proceedings_book_count = int(
                db.publications.find({"authors": aut["_id"], "type": "proceedings-book", "year": int(year)},
                                     {"authors": 1}).count())

            countSingleYearDic = {}
            if (conference_count > 0):
                countSingleYearDic.update({'conference': conference_count})
            if (journal_count > 0):
                countSingleYearDic.update({'journal': journal_count})
            if (technical_count > 0):
                countSingleYearDic.update({'technical': technical_count})
            if (workshop_count > 0):
                countSingleYearDic.update({'workshop': workshop_count})
            if (thesis_count > 0):
                countSingleYearDic.update({'thesis': thesis_count})
            if (proceedings_book_count > 0):
                countSingleYearDic.update({'proceedings-book': proceedings_book_count})

            if bool(countSingleYearDic):
                countDic.update({str(year): countSingleYearDic})

            conference_count = 0
            journal_count = 0
            technical_count = 0
            workshop_count = 0
            thesis_count = 0
            proceedings_book_count = 0


            # set pubs for each author:

            pubsList = list(db.publications.find({"authors": aut["_id"], "year": year}, {"_id": 1}))
            if bool(pubsList):
                for pub in pubsList:
                    tmpDic = {'ObjectId': str(pub["_id"]).replace('ObjectId("', '').replace('")', ''), "year": year}
                    pubs.append(tmpDic)



            # set coauthors for each author:
            authorList = list(db.publications.find({"authors": aut["_id"], "year": year}, {"_id": 0, "authors": 1}))
            if bool(authorList):
                for publishedWith in authorList:
                    for name in publishedWith["authors"]:
                        if not name in aut["_id"]:
                            # print name
                            if not coauthDic.has_key(name):
                                tmpList = []
                                tmpList.append(year)
                                coauthDic.update({str(name.encode('utf8', 'replace').replace(".", "")): tmpList})
                            else:

                                tmpList = coauthDic.get(name)
                                tmpList.append(year)

                                insertList = list(set(tmpList))
                                coauthDic.update({str(name.encode('utf8', 'replace').replace(".", "")): insertList})





        # if authors has publications update his entry in database
        if bool(countDic):
            db.authors.update({"_id": aut["_id"]}, {'$set': {"count": countDic}})
            db.authors.update({"_id": aut["_id"]}, {'$set': {"pubs": pubs}})
            db.authors.update({"_id": aut["_id"]}, {'$set': {"publishedWith": coauthDic}})


def setUpDatabase():
    db.drop_collection('authors')
    db.drop_collection('publications')

    # needs to be called in this order!
    migrateAuthors2Mongo() # creates author collection
    migratePublications2Mongo()  # creates main collection for pubs
    updateCPC()  # update relationships
    print("[INFO] Database created!")


setUpDatabase()