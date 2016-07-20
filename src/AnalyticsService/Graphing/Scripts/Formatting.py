import sys
from xml.etree import ElementTree

import networkx as nx
import os
from AnalyticsService.Graphing.GraphUtils import GraphColor

reload(sys)  # just to be sure
sys.setdefaultencoding('utf-8')

path = sys.argv[1]
c = sys.argv[2]
G = nx.read_graphml(path=path)

if c=="retweet":
    _node_color = ("type", {"status": "blue", "source": "red", "target": "lime"})
    _edges_color = ("type", {"source": "gold", "target": "turquoise"})
    color = (_node_color, _edges_color)
elif c=="community":
    _edges_color = ("type", {"retweet": "cornflowerblue"})
    _node_color = ("type", {"retweeted": "firebrick", "retweeter": "lime", "both": "darkorange"})
    color = (_node_color, _edges_color)
else:
    print "Unknown color scheme"
    exit()

GraphColor.color_graph(G, mapping=color)

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


print "Replacing"
with open("temp.graphml") as f:
    for line in f:
        for src in replacements:
            line = line.replace(src, replacements[src])
        lines.append(line)

print "Outputting"
name, ext = os.path.splitext(path)

with open(name + "_formatted"+ext, "w") as des:
    for line in lines:
        des.write(line)