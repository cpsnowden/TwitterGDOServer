# # size = \
# #     {'Unclassified': 6391.0, 'Clinton': 12887.0, 'Trump': 14439.0, 'Saunders': 7956.0}
# # retweet= \
# #     {('Trump', 'Clinton'): 1183.0, ('Unclassified', 'Unclassified'): 33.0, ('Saunders', 'Saunders'): 15718.0,
# #      ('Trump', 'Trump'): 20864.0, ('Trump', 'Unclassified'): 14.0, ('Unclassified', 'Clinton'): 643.0,
# #      ('Clinton', 'Trump'): 544.0, ('Clinton', 'Clinton'): 18466.0, ('Unclassified', 'Saunders'): 810.0,
# #      ('Saunders', 'Clinton'): 789.0, ('Saunders', 'Trump'): 251.0, ('Clinton', 'Unclassified'): 8.0,
# #      ('Saunders', 'Unclassified'): 14.0, ('Clinton', 'Saunders'): 590.0, ('Unclassified', 'Trump'): 1214.0,
# #      ('Trump', 'Saunders'): 500.0}
# # retweet_density = \
# #     {('Trump', 'Clinton'): 6.357638056956838e-06, ('Unclassified', 'Unclassified'): 8.079347813499567e-07,
# #      ('Unclassified', 'Saunders'): 1.5930207283857178e-05, ('Saunders', 'Saunders'): 0.00024831773345658536,
# #      ('Trump', 'Unclassified'): 1.517127573816411e-07, ('Unclassified', 'Clinton'): 7.807110509843535e-06,
# #      ('Clinton', 'Trump'): 2.923546156368994e-06, ('Clinton', 'Clinton'): 0.00011119088174074172,
# #      ('Saunders', 'Clinton'): 7.695385846646351e-06, ('Saunders', 'Trump'): 2.184951647803483e-06,
# #      ('Clinton', 'Unclassified'): 9.713356777410307e-08, ('Saunders', 'Unclassified'): 2.7533691601728457e-07,
# #      ('Clinton', 'Saunders'): 5.7544710386835834e-06, ('Trump', 'Saunders'): 4.352493322317695e-06,
# #      ('Unclassified', 'Trump'): 1.3155663390093735e-05, ('Trump', 'Trump'): 0.00010007447979845008}
# # mention = \
# #     {('Trump', 'Clinton'): 337.0, ('Unclassified', 'Unclassified'): 108.0, ('Saunders', 'Saunders'): 1067.0,
# #      ('Trump', 'Trump'): 1757.0, ('Trump', 'Unclassified'): 2060.0, ('Unclassified', 'Clinton'): 53.0,
# #      ('Clinton', 'Trump'): 675.0, ('Clinton', 'Clinton'): 1901.0, ('Unclassified', 'Saunders'): 14.0,
# #      ('Saunders', 'Clinton'): 362.0, ('Saunders', 'Trump'): 148.0, ('Clinton', 'Unclassified'): 5925.0,
# #      ('Saunders', 'Unclassified'): 2055.0, ('Clinton', 'Saunders'): 1107.0, ('Unclassified', 'Trump'): 40.0,
# #      ('Trump', 'Saunders'): 35.0}
# # mention_density = \
# #     {('Trump', 'Clinton'): 1.8110938505447627e-06, ('Unclassified', 'Unclassified'): 2.644150193508949e-06,
# #      ('Unclassified', 'Saunders'): 2.7533691601728457e-07, ('Saunders', 'Saunders'): 1.6856789769574794e-05,
# #      ('Trump', 'Unclassified'): 2.2323448586155763e-05, ('Unclassified', 'Clinton'): 6.435098865034328e-07,
# #      ('Clinton', 'Trump'): 3.627561866818145e-06, ('Clinton', 'Clinton'): 1.1446651477805156e-05,
# #      ('Saunders', 'Clinton'): 3.530709349158402e-06, ('Saunders', 'Trump'): 1.2883380234060377e-06,
# #      ('Clinton', 'Unclassified'): 7.193954863269508e-05, ('Saunders', 'Unclassified'): 4.0415525886822836e-05,
# #      ('Clinton', 'Saunders'): 1.0796948203089367e-05, ('Trump', 'Saunders'): 3.046745325622387e-07,
# #      ('Unclassified', 'Trump'): 4.3346502109040316e-07, ('Trump', 'Trump'): 8.427476083487192e-06}
# # both = \
# #     {('Trump', 'Clinton'): 2.0, ('Unclassified', 'Unclassified'): 0.0, ('Saunders', 'Saunders'): 50.0,
# #      ('Trump', 'Trump'): 47.0, ('Trump', 'Unclassified'): 0.0, ('Unclassified', 'Clinton'): 0.0,
# #      ('Clinton', 'Trump'): 1.0, ('Clinton', 'Clinton'): 73.0, ('Unclassified', 'Saunders'): 0.0,
# #      ('Saunders', 'Clinton'): 0.0, ('Saunders', 'Trump'): 0.0, ('Clinton', 'Unclassified'): 0.0,
# #      ('Saunders', 'Unclassified'): 0.0, ('Clinton', 'Saunders'): 9.0, ('Unclassified', 'Trump'): 1.0,
# #      ('Trump', 'Saunders'): 1.0}
# #
# # both_density = \
# #     {('Trump', 'Clinton'): 1.0748331457238949e-08, ('Unclassified', 'Unclassified'): 0.0,
# #      ('Unclassified', 'Saunders'): 0.0, ('Saunders', 'Saunders'): 7.899151719575817e-07, ('Trump', 'Unclassified'): 0.0,
# #      ('Unclassified', 'Clinton'): 0.0, ('Clinton', 'Trump'): 5.3741657286194745e-09,
# #      ('Clinton', 'Clinton'): 4.395610509625336e-07, ('Saunders', 'Clinton'): 0.0, ('Saunders', 'Trump'): 0.0,
# #      ('Clinton', 'Unclassified'): 0.0, ('Saunders', 'Unclassified'): 0.0,
# #      ('Clinton', 'Saunders'): 8.778006669178347e-08, ('Trump', 'Saunders'): 8.70498664463539e-09,
# #      ('Unclassified', 'Trump'): 1.0836625527260079e-08, ('Trump', 'Trump'): 2.254361843619226e-07}
# #
# # from pprint import pprint as pr
# #
# # print "Size"
# # pr(sorted(size.items(), key= lambda x:x[1], reverse=True))
# # print "Retweet"
# # pr(sorted(retweet.items(), key= lambda x:x[1], reverse=True))
# # print "Mention"
# # pr(sorted(mention.items(), key= lambda x:x[1], reverse=True))
# # print "Both"
# # pr(sorted(both.items(), key= lambda x:x[1], reverse=True))
#
#
# import networkx as nx
# import community
#
# G = nx.Graph()
#
# G.add_path([1,2,3,4])
# G.add_path([2,8,9])
# G.add_path([5,6,7,8])
#
# G.add_path([11,12])
# print G.nodes()
# G = max(nx.connected_component_subgraphs(G), key=len)
# print G.nodes()
# # partitions = community.best_partition(G, resolution=1)
# # communities = set(partitions.values())
# # print partitions
import json
# import pprint
# from collections import OrderedDict
#
# with open("out_retweet.txt") as f:
#     data = json.load(f)
#     s, count, density = data
#     print s
#     d = (OrderedDict([((c[0][0],c[0][1]),c[1] ) for c in count]))
#     c = (OrderedDict([((c[0][0],c[0][1]),c[1] ) for c in density]))
#     e = {}
#     for i in d.keys():
#         e[i] = (d[i], c[i])
#
# for key in e:
#     print key[0], key[1], e[key][0], e[key][1]
#
# #
#
with open("per_classification.txt") as f:
    data = json.load(f)

    print data

import numpy as np
import matplotlib.pyplot as plt

sizes = []
second_difference = []

for i,d in data.values():
    j =  sorted(d.items(),key = lambda x:x[1], reverse=True)
    sizes.append(sum(d.values()))
    if len(j) > 1:
        second_difference.append((j[0][1] - j[1][1]) / float(sum(d.values())))
    else:
        second_difference.append(j[0][1] / float(sum(d.values())))
    print j

sizes, second_difference = (list(x) for x in zip(*sorted(zip(sizes, second_difference))))

plt.plot(sizes, second_difference)
plt.show()

# fig,axes = plt.subplots(len(data))
# width = 0.7
# for i,d in enumerate(data.values()):
#     print d
#     dat = []
#     for v in sorted(d[1].keys()):
#         dat.append(d[1][v])
#     axes[i].set_title(d[0])
#     axes[i].bar(range(len(dat)),dat, width)
#     axes[i].set_xticks(np.arange(len(dat)) + width/float(2))
#     axes[i].set_xticklabels(tuple(sorted(d[1].keys())))
#
#
# plt.show()