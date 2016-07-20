import json
import logging
from collections import Counter
from itertools import product

import community

import networkx as nx
from AnalyticsService.Graphing.Classification.Classification import CommunityUser
from AnalyticsService.Graphing.Community.CommunityGraph import CommunityGraph
from AnalyticsService.TwitterObj import Status
from dateutil import parser


class CommunityGraphClassification(CommunityGraph):
    _logger = logging.getLogger(__name__)

    __type_name = "Community Graph Classification"
    __arguments = [{"name": "resolution", "prettyName": "Community Detection Resolution", "type": "integer",
                    "default": 1.0},
                   {"name": "fractionToClassify", "prettyName": "Fraction of nodes to classify", "type": "integer",
                    "default": 0.05},
                   {"name": "retweet", "prettyName": "Retweet Edges", "type": "boolean",
                    "default": True},
                   {"name": "mention", "prettyName": "Mention Edges", "type": "boolean",
                    "default": True},
                   {"name": "hashtag_grouping", "prettyName":"Hashtag Groupings", "type":"dictionary_list",
                    "variable":True,"default": [
                        {"name":"Trump", "tags": ["makeamericagreatagain", "trump2016"], "color":"blue"},
                        {"name": "Saunders", "tags": ["feelthebern"], "color":"lime"},
                        {"name": "Clinton", "tags": ["imwithher"], "color":"red"}]}]

    _edges_color = ("type", {"retweet": "blue", "mention": "gold", "both": "red"})
    _node_color = ("type", {"retweeted": "red", "retweeter": "lime", "both": "blueviolet"})

    @classmethod
    def get_color(cls):
        return cls._node_color, cls._edges_color

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(CommunityGraph, cls).get_args()

    @classmethod
    def get_type(cls):
        return cls.__type_name

    ####################################################################################################################


    @classmethod
    def get(cls, analytics_meta):

        gridfs, db_col, args, schema_id = cls.setup(analytics_meta)

        tweet_limit = args["tweetLimit"]
        start_date_co = parser.parse(args["startDateCutOff"])
        end_date_co = parser.parse(args["endDateCutOff"])
        resolution = args["resolution"]
        mention_edges = args["mention"]
        retweet_edges = args["retweet"]
        fraction = args['fractionToClassify']

        scores = dict([(i["name"],i["tags"]) for i in args["hashtag_grouping"]])
        node_colors = ("classification", dict([(i["name"],i["color"]) for i in args["hashtag_grouping"]]))

        cls._logger.info("Got the following tags dictionary: %s", scores)
        cls._logger.info("Include mention edges: %s", mention_edges)
        cls._logger.info("Include retweet edges: %s", retweet_edges)

        query = {}

        cursor = cls.get_cursor(db_col, query, schema_id, tweet_limit)
        if cursor is None:
            analytics_meta.status = "NO DATA IN RANGE"
            analytics_meta.save()
            return

        graph, users = cls.build_graph(cursor, schema_id, retweet_edges, mention_edges, scores)

        graph = max(nx.connected_component_subgraphs(graph.to_undirected()), key=len)

        graph = cls.community_detect(graph, users, resolution, fraction, scores)
        # cls.analyse_inter_community(graph, "retweet", scores)
        # cls.analyse_inter_community(graph, "mention", scores)
        # cls.analyse_inter_community(graph, "both", scores)

        cls._logger.info("Build graph %s", analytics_meta.id)
        analytics_meta.status = "BUILT"
        analytics_meta.save()

        graph = cls.layout(graph, analytics_meta,gridfs, args)
        cls.finalise_graph(graph, gridfs, analytics_meta, (node_colors, cls._edges_color))

        return True

    # @classmethod
    # def analyse_inter_community(cls, graph, edge_type, scores):
    #
    #     cls._logger.info("Analysing edge type: %s", edge_type)
    #     classifications = scores.keys() + [CommunityUser.UNCLASSIFIED]
    #     c_to_c = product(classifications, classifications)
    #     iter_community = dict.fromkeys(c_to_c, 0.0)
    #     iter_community_size = dict.fromkeys(classifications, 0.0)
    #
    #     for node, d in graph.nodes(data=True):
    #         classification = d["classification"]
    #         iter_community_size[classification] += 1.0
    #
    #     for u, v, d in graph.edges_iter(data=True):
    #         if d["type"] != edge_type:
    #             continue
    #         source_node_cls = graph.node[u]["classification"]
    #         target_node_cls = graph.node[v]["classification"]
    #         iter_community[(source_node_cls, target_node_cls)] += d["number_tweets"]
    #
    #     iter_community_density = dict.fromkeys(c_to_c, 0.0)
    #     for (s, t) in iter_community.keys():
    #         # if s==t:
    #         #     normaliser = float(iter_community_size[s] * (iter_community_size[t]))
    #         # else:
    #         if iter_community_size[s] == 0 or iter_community_size[t] == 0:
    #             normaliser = 1.0
    #         else:
    #             normaliser = float(iter_community_size[s] * iter_community_size[t])
    #
    #         iter_community_density[(s, t)] = iter_community[(s, t)] / normaliser
    #
    #     a = sorted(iter_community_size.items(), key=lambda x: x[1], reverse=True)
    #     b = sorted(iter_community.items(), key=lambda x: x[1], reverse=True)
    #     c = sorted(iter_community_density.items(), key=lambda x: x[1], reverse=True)
    #
    #     json.dump([a, b, c], open("out_" + edge_type + ".txt", "w"))

    @classmethod
    def build_graph(cls, cursor, schema_id, retweet_edges, mention_edges, scores):

        graph = nx.DiGraph()
        users = {}

        for status_json in cursor:

            status = Status(status_json, schema_id)

            # Add the source user
            source_user = status.get_user()
            source_user_id = str(source_user.get_id())

            if source_user_id not in users:
                users[source_user_id] = CommunityUser(classification_scores=scores)
                graph.add_node(source_user_id, name=source_user.get_name())

            source_user_obj = users[source_user_id]
            source_user_obj.said(status)

            graph.node[source_user_id]["no_statuses"] = source_user_obj.get_number_statuses()

            retweet = status.get_retweet_status()
            if retweet_edges and retweet is not None:
                retweeted_user = retweet.get_user()
                retweeted_user_id = str(retweeted_user.get_id())

                if retweeted_user_id not in users:
                    users[retweeted_user_id] = CommunityUser(classification_scores=scores)
                    graph.add_node(retweeted_user_id, name=retweeted_user.get_name())

                user_obj = users[retweeted_user_id]
                user_obj.retweeted_by(source_user_obj)
                user_obj.said(retweet)
                graph.node[retweeted_user_id]["no_statuses"] = user_obj.get_number_statuses()
                graph.node[retweeted_user_id]["no_times_retweeted"] = user_obj.number_of_times_retweets

                if not graph.has_edge(source_user_id, retweeted_user_id):
                    graph.add_edge(source_user_id, retweeted_user_id, type="retweet", number_tweets = 1)
                else:
                    graph[source_user_id][retweeted_user_id]["number_tweets"] += 1
                    if graph[source_user_id][retweeted_user_id]["type"] == "mention":
                        graph[source_user_id][retweeted_user_id]["type"] = "both"

            elif mention_edges:
                for mention in status.get_mentions():

                    mentioned_user_id = str(mention.get_id())

                    if mentioned_user_id not in users:
                        users[mentioned_user_id] = CommunityUser(classification_scores=scores)
                        graph.add_node(mentioned_user_id, name=mention.get_name())

                    user_obj = users[mentioned_user_id]
                    user_obj.mentioned()
                    graph.node[mentioned_user_id]["no_times_mentioned"] = user_obj.number_of_times_mentioned

                    if not graph.has_edge(source_user_id, mentioned_user_id):
                        graph.add_edge(source_user_id, mentioned_user_id, type="mention", number_tweets = 1)
                    else:
                        graph[source_user_id][mentioned_user_id]["number_tweets"] += 1
                        if graph[source_user_id][mentioned_user_id]["type"] == "retweet":
                            graph[source_user_id][mentioned_user_id]["type"] = "both"

        return graph, users

    @classmethod
    def community_detect(cls, graph, users, resolution, fraction, scores):

        partitions = community.best_partition(graph.to_undirected(), resolution=resolution)

        counter = Counter(partitions.values())
        number_of_nodes = sum(counter.values())

        cls._logger.info("Counter %s", counter)

        communities = [i for i in counter.items() if i[1] > fraction * number_of_nodes]

        cls._logger.info("Number of nodes: %d", number_of_nodes)
        cls._logger.info("Number of communities to map: %d", len(communities))
        cls._logger.info("Communities: %s", communities)

        partitions_to_com = dict.fromkeys(set(partitions.values()), CommunityUser.UNCLASSIFIED)

        output = {}

        for com,_ in communities:
            com_nodes = [users[n].get_classification for n in partitions.keys() if partitions[n] == com]
            com_classes = Counter(com_nodes)

            cls._logger.info("%d: %s", com, com_classes)
            partitions_to_com[com] = com_classes.most_common(1)[0][0]
            output[com] = (partitions_to_com[com],com_classes)

        for node in graph.nodes():
            c = partitions[node]
            graph.node[node]["community"] = c
            graph.node[node]["classification"] = partitions_to_com[c]

        json.dump(output, open("per_classification.txt","w"))

        return graph
