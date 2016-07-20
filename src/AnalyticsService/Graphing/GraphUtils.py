import logging

from xml.etree import ElementTree
import sys
import numpy as np

reload(sys)  # just to be sure
sys.setdefaultencoding('utf-8')

from matplotlib import colors


class GraphUtils(object):
    _logger = logging.getLogger(__name__)

    @classmethod
    def fix_graphml_format(cls, name, gridfs):

        cls._logger.info("Reformatting file to fix GDO graphml formatting")

        ns = {"graphml": "http://graphml.graphdrawing.org/xmlns"}

        lines = []
        with gridfs.get_last_version(name) as f:
            tree = ElementTree.parse(f)
            f_id = f._id

            replacements = {}

            for key_entry in tree.findall("graphml:key", ns):
                id = '"' + key_entry.attrib['id'] + '"'
                replacements[id] = ('"' + key_entry.attrib['attr.name'] + '"').replace(" ", "")

        with gridfs.get_last_version(name) as f:
            for line in f:
                for src in replacements:
                    line = line.replace(src, replacements[src])
                lines.append(line)

        with gridfs.new_file(filename=name, content_type="text/xml") as des:
            for line in lines:
                des.write(line)

        cls._logger.info("Deleting old version of file %s", f_id)
        gridfs.delete(f_id)

    @classmethod
    def maximise_aspect(cls, graph):

        node_dictionary = dict(graph.nodes(data=True))

        coords = {}
        for node_id in node_dictionary.keys():
            x = node_dictionary[node_id]["x"]
            y = node_dictionary[node_id]["y"]
            coords[node_id] = (x, y)

        centroid = tuple(map(np.mean, zip(*coords.values())))

        for node_id in coords.keys():
            c = coords[node_id]
            coords[node_id] = (c[0] - centroid[0], c[1] - centroid[1])

        ratio = 1
        best_theta = 0
        best_coords = coords

        for theta in np.arange(0, 355, 5):

            rotated_coords = {}

            for node_id in coords:
                x, y = coords[node_id]
                new_x = x * np.cos(np.deg2rad(theta)) + y * np.sin(np.deg2rad(theta))
                new_y = - x * np.sin(np.deg2rad(theta)) + y * np.cos(np.deg2rad(theta))
                rotated_coords[node_id] = (new_x, new_y)

            min_x = min(rotated_coords.items(), key=lambda x: x[1][0])[1][0]
            max_x = max(rotated_coords.items(), key=lambda x: x[1][0])[1][0]
            min_y = min(rotated_coords.items(), key=lambda x: x[1][1])[1][1]
            max_y = max(rotated_coords.items(), key=lambda x: x[1][1])[1][1]

            x_range = max_x - min_x
            y_range = max_y - min_y
            new_ratio = y_range / x_range

            if new_ratio > ratio:
                ratio = new_ratio
                best_theta = theta
                best_coords = rotated_coords

        cls._logger.info("Best theta = %d", best_theta)

        for node_id in best_coords:
            nc = best_coords[node_id]
            node_dictionary[node_id]["x"] = float(nc[0])
            node_dictionary[node_id]["y"] = float(nc[1])

        return graph


class GraphColor(object):
    @staticmethod
    def color_graph(graph, mapping):

        node_map, edge_map = mapping

        graph = GraphColor.color_nodes(graph, node_map)
        graph = GraphColor.color_edges(graph, edge_map)

        return graph

    @staticmethod
    def color_edges(graph, mapping):

        key, color_map = mapping

        for u, v, k in graph.edges(data=key):
            GraphColor.color_element_from_rgb(graph[u][v], color_map.get(k, "grey"))

        return graph

    @staticmethod
    def color_nodes(graph, mapping):

        key, color_map = mapping
        for nid in graph.nodes():
            node = graph.node[nid]
            try:
                t = node[key]
            except KeyError:
                # print "Could not get key on ", node
                t = "grey"
            GraphColor.color_element_from_rgb(node, color_map.get(t, "grey"))

        return graph

    @staticmethod
    def color_element_from_rgb(element, color):

        try:
            rgb = colors.colorConverter.to_rgb(color)
        except ValueError:
            rgb = (0.0, 0.0, 0.0)

        element['r'], element['g'], element['b'] = [int(i * 255) for i in rgb]
