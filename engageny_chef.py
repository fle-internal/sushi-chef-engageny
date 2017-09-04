#!/usr/bin/env python

from collections import defaultdict
import json
import logging
import os
import re
import tempfile
import shutil
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
import jinja2
import requests

from le_utils.constants import content_kinds, file_formats, licenses
from ricecooker.chefs import SushiChef
from ricecooker.classes import nodes
from ricecooker.classes import files
from ricecooker.classes.files import HTMLZipFile
from ricecooker.classes.licenses import get_license
from ricecooker.classes.nodes import ChannelNode, HTML5AppNode, TopicNode
from ricecooker.config import LOGGER
from ricecooker.exceptions import UnknownFileTypeError, raise_for_invalid_channel
from ricecooker.utils.caching import CacheForeverHeuristic, FileCache, CacheControlAdapter, InvalidatingCacheControlAdapter
from ricecooker.utils.html import download_file
from ricecooker.utils.zip import create_predictable_zip



# ENGAGE NY settings
################################################################################
ENGAGENY_CC_START_URL = 'https://www.engageny.org/common-core-curriculum'


# Set up webcaches
################################################################################
sess = requests.Session()
cache = FileCache('.webcache')
basic_adapter = CacheControlAdapter(cache=cache)
forever_adapter = CacheControlAdapter(heuristic=CacheForeverHeuristic(), cache=cache)
sess.mount('http://', basic_adapter)
sess.mount('https://', basic_adapter)
sess.mount('http://www.engageny.org', forever_adapter)
sess.mount('https://www.engageny.org', forever_adapter)

# Chef settings
################################################################################
DATA_DIR = 'chefdata'
TREES_DATA_DIR = os.path.join(DATA_DIR, 'trees')
CRAWLING_STAGE_OUTPUT = 'web_resource_tree.json'
SCRAPING_STAGE_OUTPUT = 'ricecooker_json_tree.json'


# LOGGING SETTINGS
################################################################################
# logging.getLogger("cachecontrol.controller").setLevel(logging.WARNING)
# logging.getLogger("requests.packages").setLevel(logging.WARNING)
# logger = logging.getLogger('engageny')



# HELPER FUNCTIONS
################################################################################
get_text = lambda x: "" if x is None else x.get_text().replace('\r', '').replace('\n', ' ').strip()



# CRAWLING
################################################################################

# def visit_grade
# def visit_module
# def visit_unit
# def visit_lesson


def crawling_part(args, options):
    """
    Visit all the urls on engageny.org/resource/ and extract content structure.
    """
    # crawl website to build web_resource_tree =
    web_resource_tree = dict(
        kind="EngageNYWebRessourceTree",
        title="Engage NY Web Resource Tree (ELS and CCSSM)",
        lang='NOT USED ' +  'en',
        children=[]
    )

    # DO ALL THE CRAWLING...    (see ressa chef for example)
    #
    #
    #
    #
    #

    json_file_name = os.path.join(TREES_DATA_DIR, CRAWLING_STAGE_OUTPUT)
    with open(json_file_name, 'w') as json_file:
        json.dump(web_resource_tree, json_file, indent=2)
        LOGGER.info('Crawling results stored in ' + json_file_name)




# SCRAPING
################################################################################

# def download_module
# def download_unit
# def download_lesson
# def _build_json_tree


def scraping_part(args, options):
    """
    Download all categories, subpages, modules, and resources from open.edu.
    """
    # Read web_resource_trees.json
    with open(os.path.join(TREES_DATA_DIR, CRAWLING_STAGE_OUTPUT)) as json_file:
        web_resource_tree = json.load(json_file)
        assert web_resource_tree['kind'] == 'EngageNYWebRessourceTree'

    # Ricecooker tree
    ricecooker_json_tree = dict(
        kind='ChannelNode',
        title='NOT USED ' +  web_resource_tree['title'],
        language='NOT USED ' +  web_resource_tree['lang'],
        children=[],
    )

    # DO ALL THE SCRAPING
    #
    # _build_json_tree(ricecooker_json_tree, web_resource_tree['children']) (see ressa chef for example)
    print('finished building ricecooker_json_tree')

    # Write out ricecooker_json_tree.json
    json_file_name = os.path.join(TREES_DATA_DIR, SCRAPING_STAGE_OUTPUT)
    with open(json_file_name, 'w') as json_file:
        json.dump(ricecooker_json_tree, json_file, indent=2)
        LOGGER.info('Intermediate result stored in ' + json_file_name)





# CONSTRUCT CHANNEL FROM RICECOOKER JSON TREE
################################################################################

def build_tree(parent_node, sourcetree):
    """
    Parse nodes given in `sourcetree` and add as children of `parent_node`.
    """
    EXPECTED_NODE_TYPES = ['TopicNode', 'AudioNode', 'DocumentNode', 'HTML5AppNode']

    for source_node in sourcetree:
        kind = source_node['kind']
        if kind not in EXPECTED_NODE_TYPES:
            logger.critical('Unexpected Node type found: ' + kind)
            raise NotImplementedError('Unexpected Node type found in channel json.')

        if kind == 'TopicNode':
            child_node = nodes.TopicNode(
                source_id=source_node["source_id"],
                title=source_node["title"],
                author=source_node.get("author"),
                description=source_node.get("description"),
                thumbnail=source_node.get("thumbnail"),
            )
            parent_node.add_child(child_node)
            source_tree_children = source_node.get("children", [])
            build_tree_for_language(child_node, source_tree_children)

        elif kind == 'AudioNode':
            child_node = nodes.AudioNode(
                source_id=source_node["source_id"],
                title=source_node["title"],
                license=TESSA_LICENSE,
                author=source_node.get("author"),
                description=source_node.get("description"),
                # derive_thumbnail=True,                    # video-specific data
                thumbnail=source_node.get('thumbnail'),
            )
            add_files(child_node, source_node.get("files") or [])
            parent_node.add_child(child_node)

        elif kind == 'DocumentNode':
            child_node = nodes.DocumentNode(
                source_id=source_node["source_id"],
                title=source_node["title"],
                license=TESSA_LICENSE,
                author=source_node.get("author"),
                description=source_node.get("description"),
                thumbnail=source_node.get("thumbnail"),
            )
            add_files(child_node, source_node.get("files") or [])
            parent_node.add_child(child_node)

        elif kind == 'HTML5AppNode':
            child_node = nodes.HTML5AppNode(
                source_id=source_node["source_id"],
                title=source_node["title"],
                license=TESSA_LICENSE,
                author=source_node.get("author"),
                description=source_node.get("description"),
                thumbnail=source_node.get("thumbnail"),
            )
            add_files(child_node, source_node.get("files") or [])
            parent_node.add_child(child_node)

        else:
            logger.critical("Encountered an unknown content node format.")
            continue

    return parent_node


def add_files(node, file_list):
    EXPECTED_FILE_TYPES = ['VideoFile', 'ThumbnailFile', 'HTMLZipFile', 'DocumentFile']
    for f in file_list:
        file_type = f.get('file_type')
        if file_type not in EXPECTED_FILE_TYPES:
            logger.critical(file_type)
            raise NotImplementedError('Unexpected File type found in channel json.')
        path = f.get('path')  # usually a URL, not a local path
        # handle different types of files
        if file_type == 'VideoFile':
            node.add_file(files.VideoFile(path=f['path'], ffmpeg_settings=f.get('ffmpeg_settings')))
        elif file_type == 'ThumbnailFile':
            node.add_file(files.ThumbnailFile(path=path))
        elif file_type == 'HTMLZipFile':
            node.add_file(files.HTMLZipFile(path=path, language=f.get('language')))
        elif file_type == 'DocumentFile':
            node.add_file(files.DocumentFile(path=path, language=f.get('language')))
        else:
            raise UnknownFileTypeError("Unrecognized file type '{0}'".format(f['path']))



# CHEF
################################################################################

class EngageNYChef(SushiChef):
    """
    This class takes care of downloading resources from engageny.org and uplaoding
    them to Kolibri Studio, the content curation server.
    """

    def crawl(self, args, options):
        """
        PART 1: CRAWLING
        Builds the json web redource tree --- the recipe of what is to be downloaded.
        """
        crawling_part(args, options)


    def scrape(self, args, options):
        """
        PART 2: SCRAPING
        Builds the ricecooker_json_tree needed to crate the ricecooker tree for the channel
        """
        scraping_part(args, options)


    def pre_run(self, args, options):
        """
        Run the preliminary parts.
        """
        self.crawl(args, options)
        self.scrape(args, options)


    def get_channel(self, **kwargs):
        channel = ChannelNode(
            source_domain = 'engageny.org',
            source_id = 'engagny-testing-testing',    # TODO: remove -testing
            title = 'Engage NY-testing-testing',      # TODO: remove -testing
            thumbnail = './content/engageny_logo.png',
            description = 'EngageNY Common Core Curriculum Content... ELA and CCSSM combined',
            language = 'en'
        )
        return channel

    def construct_channel(self, **kwargs):
        channel = self.get_channel(**kwargs)
        # Load ricecooker json tree data for language `lang`
        with open(os.path.join(TREES_DATA_DIR, SCRAPING_STAGE_OUTPUT)) as infile:
            json_tree = json.load(infile)
            if json_tree is None:
                raise ValueError('Could not find ricecooker json tree')
        build_tree(channel, json_tree['children'])
        raise_for_invalid_channel(channel)
        return channel



# CLI
################################################################################

if __name__ == '__main__':
    chef = EngageNYChef()
    chef.main()




