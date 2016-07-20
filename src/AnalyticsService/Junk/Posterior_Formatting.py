import sys
from xml.etree import ElementTree

from src.AnalyticsService.Graphing.GraphUtils import GraphColor

import networkx as nx
from AnalyticsService.Junk.RetweetGraph import RetweetGraph

reload(sys)  # just to be sure
sys.setdefaultencoding('utf-8')

path = "/Users/ChrisSnowden/Dropbox/1_Imperial College/Individual Project/GRAPHS/12_07_16/large_variable_strength_top_50.graphml"
output_path = "/Users/ChrisSnowden/Dropbox/1_Imperial College/Individual " \
              "Project/GRAPHS/12_07_16/large_variable_strength_top_50_formatted.graphml"

G = nx.read_graphml(path=path)

GraphColor.color_graph(G, mapping=RetweetGraph.color)

nx.write_graphml(G, "temp.graphml")

ns = {"graphml": "http://graphml.graphdrawing.org/xmlns"}

print "Trying to format"
lines = []

with open("temp.graphml") as f:
    tree = ElementTree.parse(f)

    replacements = {}

    for key_entry in tree.findall("graphml:key", ns):
        id = '"' + key_entry.attrib['id'] + '"'
        replacements[id] = ('"' + key_entry.attrib['attr.name'] + '"').replace(" ", "")

with open("temp.graphml") as f:
    for line in f:
        for src in replacements:
            line = line.replace(src, replacements[src])
        lines.append(line)

with open(output_path, "w") as des:
    for line in lines:
        des.write(line)