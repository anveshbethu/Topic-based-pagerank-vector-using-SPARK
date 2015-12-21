import re
import sys
from operator import add
from pyspark import SparkContext
def creatpair(line):
        key = line[0].split()
        value = line[1]
        a = []
        for i in key:
                a.append((i, value))
        return a
def appen(v):
        a = []
        for i in v:
                if i not in a:
                        a.append(i)
        return a
def extractUrls(line):
	value = line.replace("a href","urlbethustart")
	revalue = value.replace(".html",".html urlbethufinish")
	needed = re.findall(r'urlbethustart(.*?) urlbethufinish',revalue)
        a = []
        for i in needed:
		cleani = i.encode('ascii', 'ignore')
		if (".html" in cleani):
			if ("www" in cleani):
				cli = cleani.replace('="http://','')
			else: 
				a.append(cleani.split('/')[-1])
	b = ', '.join(a)
	return b
def appen(v):
        a = []
        for i in v:
                if i not in a:
                        a.append(i)
        b = ', '.join(a)
        return b
def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)
def flippair(line):
        key = line[1].split(',')
        value = line[0]
        a = []
        for i in key:
                a.append((i, value))
        return a
def ad((x,y)):
	t = 0
	if y is None:
		t = x
	else:
		t = x+y
	return t	
if len(sys.argv) != 6:
    print >> sys.stderr, "Usage: wordcount <html_word> <related_word> <html_url> <out_file> <iterations>"
    exit(-1)
sc = SparkContext(appName="PythonWordDocCount")
lines = sc.textFile(sys.argv[1]) 
related_words = sc.textFile(sys.argv[2])
words = related_words.map(lambda (line): (line.lower(), 1))
docwords = lines.map(lambda line: (line.split(" ", 1)[1], line.split(' ')[0]))
kvpair = docwords.flatMap(creatpair)
gropair = kvpair.map(lambda (x, y): (y.lower(), x))
related = kvpair.join(words)
word_html = related.mapValues(lambda (line, num): (line))
html_word = word_html.map(lambda (x, y): (y, 1)).reduceByKey(add)
urls = sc.textFile(sys.argv[3])
href = urls.filter(lambda line: "a href" in line)
html_href = href.map(lambda line: (line.split(' ')[0], line.split(' ', 1)[1]))
html_urls = html_href.mapValues(extractUrls)
html_urlsappen = html_urls.groupByKey().mapValues(appen)
html_urlsappen_num = html_urlsappen.join(html_word)
related_html_urls = html_urlsappen_num.mapValues(lambda (line, num): (line))
flip = related_html_urls.flatMap(flippair)
links = flip.map(lambda (x, y): (x, y)).distinct().groupByKey().cache()
aranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
left_ranks = aranks.leftOuterJoin(html_word)
ranks = left_ranks.mapValues(ad)
temp_join = links.join(ranks)
contribs = temp_join.flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
temp_aggreg = contribs.reduceByKey(add)
temp_ranks = temp_aggreg.mapValues(lambda rank: rank * 0.85 + 0.15)
for iteration in xrange(int(sys.argv[5])):
	contribs = links.join(ranks).flatMap(lambda (url, (urls, rank)): computeContribs(urls, rank))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
	for (link, rank) in ranks.collect():
		cleanlink = link.encode('ascii', 'ignore')
		print "%s has rank: %s." % (cleanlink, rank)
fliprank = ranks.map(lambda (x, y): (y, x)).sortByKey(ascending=False)
frank = fliprank.map(lambda (x, y): (y, x))
frank.repartition(1).saveAsTextFile(sys.argv[4])
sc.stop()

