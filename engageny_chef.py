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

from re import compile

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
ENGAGENY_LICENSE = get_license(licenses.CC_BY_NC_SA, copyright_holder='Engage NY')


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
logging.getLogger("cachecontrol.controller").setLevel(logging.WARNING)
logging.getLogger("requests.packages").setLevel(logging.WARNING)
from ricecooker.config import LOGGER
LOGGER.setLevel(logging.DEBUG)




# HELPER FUNCTIONS
################################################################################
get_text = lambda x: "" if x is None else x.get_text().replace('\r', '').replace('\n', ' ').strip()

def get_parsed_html_from_url(url, *args, **kwargs):
    response = sess.get(url, *args, **kwargs)
    if response.status_code != 200:
        LOGGER.error("NOT FOUND:", url)
    elif not response.from_cache:
        LOGGER.debug("NOT CACHED:", url)
    return BeautifulSoup(response.content, "html.parser")


def make_fully_qualified_url(url):
    if url.startswith("//"):
        print('unexpecded // url', url)
        return "https:" + url
    elif url.startswith("/"):
        return "https://www.engageny.org" + url
    return url

# CRAWLING
################################################################################

def download_module(): # for ELA
    pass

CONTENT_OR_RESOURCE_URL_RE = compile(r'/(content|resource)/*')
def crawl(root_url):
    doc = get_parsed_html_from_url(root_url)
    dual_toc_div = doc.find('div', id='mini-panel-common_core_curriculum')
    ela_toc = dual_toc_div.find('div', class_='panel-col-first')
    math_toc = dual_toc_div.find('div', class_='panel-col-last')
    return visit_grades(ela_toc, math_toc)

def visit_grades(ela_toc, math_toc):
    ela_grades = []
    math_grades = []
    for grade_a in math_toc.find_all('a', attrs={'href': CONTENT_OR_RESOURCE_URL_RE }):
        grade_path = grade_a['href']
        grade_url = make_fully_qualified_url(grade_path)
        math_grades.append({
            'kind': 'EngageNYGrade',
            'url': grade_url,
            'title': get_text(grade_a),
            'modules': []
        })
    for grade in math_grades:
        visit_grade(grade)

    return (ela_grades, math_grades)

MODULE_URL_RE = compile(r'^/resource/(.)+-module-(\d)+$')
def visit_grade(grade):
    grade_page = get_parsed_html_from_url(grade['url'])
    grade_curriculum_toc = grade_page.find('div', class_='nysed-book-outline curriculum-map')
    for module_li in grade_curriculum_toc.find_all('li', class_='module'):
        visit_module(grade, module_li)

def visit_module(grade, module_li):
    details_div  = module_li.find('div', class_='details')
    details = details_div.find('a', attrs={'href': MODULE_URL_RE })
    grade_module = {
        'kind': 'EngageNYModule',
        'title': get_text(details),
        'url': make_fully_qualified_url(details['href']),
        'topics': [],
    }
    for topic_li in module_li.find('div', class_='tree').find_all('li', class_='topic'):
        visit_topic(grade_module['topics'], topic_li)
    grade['modules'].append(grade_module)


def visit_unit():
    pass

TOPIC_URL_RE = compile(r'^(.)+-topic(.)*')
def visit_topic(topics, topic_li):
    details_div = topic_li.find('div', class_='details')
    details = details_div.find('a', attrs={'href': TOPIC_URL_RE })
    topic = {
        'kind': 'EngageNYTopic',
        'title': get_text(details),
        'url': make_fully_qualified_url(details['href']),
        'lessons': [],
    }
    for lesson_li in topic_li.find('div', class_='tree').find_all('li', class_='lesson'):
        visit_lesson(topic, lesson_li)
    topics.append(topic)

LESSON_URL_RE = compile(r'^(.)+-lesson(.)*')
def visit_lesson(topic, lesson_li):
    details_div = lesson_li.find('div', class_='details')
    details = details_div.find('a', attrs={ 'href': LESSON_URL_RE })
    lesson = {
        'kind': 'EngageNYLesson',
        'title': get_text(details),
        'url': make_fully_qualified_url(details['href'])
    }
    topic['lessons'].append(lesson)

def crawling_part():
    """
    Visit all the urls on engageny.org/resource/ and engageny.org/content, and extract content structure.
    """
    # crawl website to build web_resource_tree
    ela_hierarchy, math_hierarchy = crawl(ENGAGENY_CC_START_URL)
    web_resource_tree = dict(
        kind="EngageNYWebResourceTree",
        title="Engage NY Web Resource Tree (ELS and CCSSM)",
        language='en',
        children = {
            'math': {
                'grades': math_hierarchy,
            },
            'ela': {
                'grades': ela_hierarchy,
            },
        },
    )
    json_file_name = os.path.join(TREES_DATA_DIR, CRAWLING_STAGE_OUTPUT)
    with open(json_file_name, 'w') as json_file:
        json.dump(web_resource_tree, json_file, indent=2)
        LOGGER.info('Crawling results stored in ' + json_file_name)

    return web_resource_tree


# SCRAPING
################################################################################

# def download_ela_grade
# def download_ela_module
# def download_ela_unit
# def download_ela_lesson

def download_math_grades(channel_tree, grades):
    for grade in grades:
        download_math_grade(channel_tree, grade)

def get_description(markup_node):
    return get_text(markup_node.find('div', 'content-body'))

def download_math_grade(channel_tree, grade):
    url = grade['url']
    grade_page = get_parsed_html_from_url(url)
    topic_node = dict(
        kind='TopicNode',
        source_id=url,
        title=grade['title'],
        description=get_description(grade_page),
        children=[],
    )
    for mod in grade['modules']:
        download_math_module(topic_node, mod)
    channel_tree['children'].append(topic_node)

def get_thumbnail_url(module_page):
    return module_page.find('meta', property='og:image')['content']

END_OF_MODULE_ASSESSMENT_RE = compile(r'(.)+-as{1,2}es{1,2}ments{0,1}.(zip|pdf)(.)*')

def get_end_of_module_assessment_url(page):
    return page.find('a', attrs={ 'href': END_OF_MODULE_ASSESSMENT_RE })

def download_math_module(topic_node, mod):
    url = mod['url']
    module_page = get_parsed_html_from_url(url)
    description = get_description(module_page)

    overview_node = dict(
        kind='DocumentNode',
        source_id=url,
        title=mod['title'],
        author='ENGAGE NY',
        description=get_description(module_page),
        thumbnail=get_thumbnail_url(module_page),
    )
    end_of_module_assessment_anchor = get_end_of_module_assessment_url(module_page)
    if end_of_module_assessment_anchor is None:
        print(url)
    assessment_node = dict(
        kind='DocumentNode',
        source_id=make_fully_qualified_url(end_of_module_assessment_anchor['href']),
        author='ENGAGE NY',
        title=get_text(end_of_module_assessment_anchor),
        description=end_of_module_assessment_anchor['title'],
    )
    module_node = dict(
        kind='TopicNode',
        source_id=url,
        title=mod['title'],
        description=description,
        children=[overview_node, assessment_node],
    )
    for topic in mod['topics']:
        download_math_topic(module_node, topic)
    topic_node['children'].append(module_node)

def download_math_topic(module_node, topic):
    pass

def download_math_lesson(lesson_url):
    doc = get_parsed_html_from_url(lesson_url)
    #
    title_h2 = doc.find('div', class_='pane-title').find('h2')
    title = get_text(title_h2)
    #
    content_div = doc.find('div', class_="pane-content")
    description = get_text(content_div)

    lesson_data = dict(
        kind='TopicNode',
        source_id=lesson_url, # TODO: extract only /path
        title=title,
        description=description,
        language='en',
        children=[],
    )

    resources_pane = doc.find('div', class_='pane-downloadable-resources')
    resources_table = resources_pane.find('table')
    resources_rows = resources_table.find_all('tr')

    for row in resources_rows:
        doc_link = row.find_all('td')[1].find('a')
        # get document source_id from row.find_all('td')[1].find('a')['href]  e.g. 44251
        title = doc_link['title'].replace('Download ','')
        doc_path = make_fully_qualified_url(doc_link['href']).split('?')[0]
        description = get_text(doc_link)
        if 'pdf' in doc_path:
            document_node = dict(
                kind='DocumentNode',
                source_id=lesson_url+":"+title, # FIXME
                title=title,
                author='Engage NY',
                description=description,
                thumbnail=None,
                files=[dict(
                    file_type='DocumentFile',
                    path=doc_path,
                    language='en',
                )],
                language='en',
            )
            lesson_data['children'].append(document_node)

    return lesson_data



def build_scraping_json_tree(web_resource_tree):
    channel_tree = dict(
        kind='ChannelNode',
        title='NOT USED ' +  web_resource_tree['title'],
        language=web_resource_tree['language'],
        children=[],
    )
    download_math_grades(channel_tree, web_resource_tree['children']['math']['grades'])
    return channel_tree

def scraping_part():
    """
    Download all categories, subpages, modules, and resources from engageny.
    """
    # Read web_resource_trees.json
    with open(os.path.join(TREES_DATA_DIR, CRAWLING_STAGE_OUTPUT)) as json_file:
        web_resource_tree = json.load(json_file)
        assert web_resource_tree['kind'] == 'EngageNYWebResourceTree'

    # Build a Ricecooker tree from scraping process
    ricecooker_json_tree = build_scraping_json_tree(web_resource_tree)

    # sample node (should be three folders deep... but for now putting in root)
    # sample_lesson_url = 'https://www.engageny.org/resource/grade-6-mathematics-module-4-topic-f-lesson-18'
    # lesson_node = download_math_lesson(sample_lesson_url)
    # ricecooker_json_tree['children'].append(lesson_node)

    LOGGER.info('Finished building ricecooker_json_tree')

    # Write out ricecooker_json_tree.json
    json_file_name = os.path.join(TREES_DATA_DIR, SCRAPING_STAGE_OUTPUT)
    with open(json_file_name, 'w') as json_file:
        json.dump(ricecooker_json_tree, json_file, indent=2)
        LOGGER.info('Scraping result stored in ' + json_file_name)

    return ricecooker_json_tree


# CONSTRUCT CHANNEL FROM RICECOOKER JSON TREE
################################################################################
# Note: the functions below are used in several chefs so might become part of `ricecooker`

def build_tree(parent_node, sourcetree):
    """
    Parse nodes given in `sourcetree` list and add as children of `parent_node`.
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
            build_tree(child_node, source_tree_children)

        elif kind == 'AudioNode':
            child_node = nodes.AudioNode(
                source_id=source_node["source_id"],
                title=source_node["title"],
                license=ENGAGENY_LICENSE,
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
                license=ENGAGENY_LICENSE,
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
                license=ENGAGENY_LICENSE,
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
    This class takes care of downloading resources from engageny.org and uploading
    them to Kolibri Studio, the content curation server.
    """

    def crawl(self, args, options):
        """
        PART 1: CRAWLING
        Builds the json web resource tree --- the recipe of what is to be downloaded.
        """
        crawling_part()


    def scrape(self, args, options):
        """
        PART 2: SCRAPING
        Builds the ricecooker_json_tree needed to create the ricecooker tree for the channel
        """
        scraping_part()


    def pre_run(self, args, options):
        """
        Run the preliminary parts.
        """
        self.crawl(args, options)
        self.scrape(args, options)


    def get_channel(self, **kwargs):
        """
        Returns a ChannelNode that contains all required channel metadata.
        """
        channel = ChannelNode(
            source_domain = 'engageny.org',
            source_id = 'engagny-testing',    # TODO: remove -testing
            title = 'Engage NY-testing',      # TODO: remove -testing
            thumbnail = './content/engageny_logo.png',
            description = 'EngageNY Common Core Curriculum Content... ELA and CCSSM combined',
            language = 'en'
        )
        return channel


    def construct_channel(self, **kwargs):
        """
        Build the channel tree by adding TopicNodes and ContentNode children.
        """
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
