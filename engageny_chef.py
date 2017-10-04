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
import zipfile
import io

from le_utils.constants import content_kinds, licenses
from ricecooker.chefs import JsonTreeChef
from ricecooker.classes.licenses import get_license
from ricecooker.utils.caching import CacheForeverHeuristic, FileCache, CacheControlAdapter, InvalidatingCacheControlAdapter
from ricecooker.utils.html import download_file
from ricecooker.utils.jsontrees import write_tree_to_json_tree
from ricecooker.utils.zip import create_predictable_zip

from pathlib import PurePosixPath

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
PDFS_DATA_DIR = os.path.join(DATA_DIR, 'pdfs')
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
def get_text(x):
    return "" if x is None else x.get_text().replace('\r', '').replace('\n', ' ').strip()

STRIP_BYTESIZE_RE = compile(r'^(.*)\s+\((\d+|\d+\.\d+)\s+\w+B\)')
def strip_byte_size(s):
    m = STRIP_BYTESIZE_RE.match(s)
    if not m:
        raise Exception('STRIP_BYTESIZE_RE did not match')
    return m.group(1)

def get_suffix(path):
    return PurePosixPath(path).suffix

MODULE_LEVEL_FILENAME_RE = compile(r'^.+/.+/.+/(?:Module\sLevel\sDocuments/){0,1}(?P<grade>\d+)(?P<moduleletter>\w)(?P<modulenumber>\w+)\.(?P<name>\D+)\.pdf$')
def get_name_and_dict_from_file_path(file_path):
    m = MODULE_LEVEL_FILENAME_RE.match(file_path)
    if not m:
        raise Exception('MODULE_LEVEL_FILENAME_RE could not match')

    grade, module_letter, module_number, name = m.groups()
    title = f'Grade {grade} '
    if module_letter == 'm':
        title += f"module {module_number}"
    if name == 'module':
        title += " overview"
    else:
        title += " " + name
    title = title.title()
    return name.lower(), dict(
        kind=content_kinds.DOCUMENT,
        source_id=file_path,
        title=title,
        description=title,
        license=ENGAGENY_LICENSE.as_dict(),
        files=[
            dict(
                file_type=content_kinds.DOCUMENT,
                path=file_path
            )
        ],
    )

UNIT_LEVEL_FILENAME_RE = compile(r'^.*(?P<grade>\d+)(?P<moduleletter>\w+)(?P<modulenumber>\d+)\.(?P<unitnumber>\d+)(?P<name>\D+)\.pdf$')
def get_name_and_dict_from_unit_file_path(file_path):
    m = UNIT_LEVEL_FILENAME_RE.match(file_path)
    if not m:
        return None

    grade, module_letter, module_number, unit_number, name = m.groups()
    title = f'Grade {grade} '
    if module_letter == 'm':
        title += f"module {module_number} Unit {unit_number}"
    if name == 'unit':
        title += " Overview"
    else:
        title += " " + name

    title = title.title()
    return name.lower(), dict(
        kind=content_kinds.DOCUMENT,
        source_id=file_path,
        title=title,
        description=title,
        license=ENGAGENY_LICENSE.as_dict(),
        files=[
            dict(
                file_type=content_kinds.DOCUMENT,
                path=file_path
            )
        ],
    )


ITEM_FROM_BUNDLE_RE = compile(r'^.+/(?P<area>.+(-i+){0,1})-(?P<grade>.+)-(?P<module>.+)-(?P<assessment_cutoff>.+-){0,1}(?P<level>.+)-(?P<type>.+)\..+$')
def get_item_from_bundle_title(path):
    m = ITEM_FROM_BUNDLE_RE.match(path)
    if m:
        return ' '.join(filter(lambda x: x is not None, m.groups())).title()
    raise Exception('Regex to match bundle item filename did not match')

def get_parsed_html_from_url(url, *args, **kwargs):
    response = sess.get(url, *args, **kwargs)
    if response.status_code != 200:
        LOGGER.error("STATUS: {}, URL: {}", response.status_code, url)
    elif not response.from_cache:
        LOGGER.debug("NOT CACHED:", url)
    return BeautifulSoup(response.content, "html.parser")

def download_zip_file(url):
    if not url:
        return (False, None)

    if get_suffix(url) != '.zip':
        return (False, None)

    response = sess.get(url)
    if response.status_code != 200:
        LOGGER.error("STATUS: {}, URL: {}", response.status_code, url)
        return (False, None)
    elif not response.from_cache:
        LOGGER.debug("NOT CACHED:", url)

    archive = zipfile.ZipFile(io.BytesIO(response.content))
    archive_members = list(filter(lambda f: f.filename.endswith('.pdf'), archive.infolist()))
    archive_member_names = [None] * len(archive_members)
    for i, pdf in enumerate(archive_members):
        path = os.path.join(PDFS_DATA_DIR, pdf.filename)
        archive_member_names[i] = path
        if not os.path.exists(path):
            archive.extract(pdf, PDFS_DATA_DIR)
    return (True, archive_member_names)

def strip_token(url):
    return url.split('?')[0]

def make_fully_qualified_url(url):
    if url.startswith("//"):
        print('unexpected // url', url)
        return strip_token("https:" + url)
    elif url.startswith("/"):
        return strip_token("https://www.engageny.org" + url)
    return strip_token(url)

# CRAWLING
################################################################################

CONTENT_OR_RESOURCE_URL_RE = compile(r'/(content|resource)/*')
def crawl(root_url):
    doc = get_parsed_html_from_url(root_url)
    dual_toc_div = doc.find('div', id='mini-panel-common_core_curriculum')
    ela_toc = dual_toc_div.find('div', class_='panel-col-first')
    math_toc = dual_toc_div.find('div', class_='panel-col-last')
    return visit_grades(ela_toc, math_toc)

def find_grades(toc, children_label='modules'):
    grades = []
    for grade in toc.find_all('a', attrs={'href': CONTENT_OR_RESOURCE_URL_RE }):
        grade_path = grade['href']
        grade_url = make_fully_qualified_url(grade_path)
        grades.append({
            'kind': 'EngageNYGrade',
            'url': grade_url,
            'title': get_text(grade),
            children_label: []
        })
    return grades

def visit_grades(ela_toc, math_toc):
    ela_grades = find_grades(ela_toc,  children_label='strands_or_modules')
    math_grades = find_grades(math_toc)

    for grade in ela_grades:
        visit_ela_grade(grade)
    for grade in math_grades:
        visit_grade(grade)

    return (ela_grades, math_grades)

STRAND_OR_MODULE_RE = compile('\w*\s*(strand|module)\s*\w*')
def visit_ela_grade(grade):
    grade_page = get_parsed_html_from_url(grade['url'])
    grade_curriculum_toc = grade_page.find('div', class_='nysed-book-outline curriculum-map')
    for strand_or_module_li in grade_curriculum_toc.find_all('li', attrs={'class': STRAND_OR_MODULE_RE}):
        visit_ela_strand_or_module(grade, strand_or_module_li)

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

RESOURCE_RE = compile(r'^/resource')
DOMAIN_OR_UNIT_RE = compile(r'\w*\s*(domain|unit)\s*\w*')
def visit_ela_strand_or_module(grade, strand_or_module_li):
    details_div = strand_or_module_li.find('div', class_='details')
    details = details_div.find('a',  attrs={'href': RESOURCE_RE})
    grade_strand_or_module = {
        'kind': 'EngageNYStrandOrModule',
        'title': get_text(details),
        'url': make_fully_qualified_url(details['href']),
        'domains_or_units': []
    }
    for domain_or_unit in strand_or_module_li.find('div', class_='tree').find_all('li', attrs={'class': DOMAIN_OR_UNIT_RE}):
        visit_ela_domain_or_unit(grade_strand_or_module, domain_or_unit)
    grade['strands_or_modules'].append(grade_strand_or_module)

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

DOCUMENT_OR_LESSON_RE = compile(r'\w*\s*(document|lesson)\w*\s*')
def visit_ela_domain_or_unit(grade_strand_or_module, domain_or_unit_li):
    details_div = domain_or_unit_li.find('div', class_='details')
    details = details_div.find('a', attrs={'href': RESOURCE_RE })
    domain_or_unit = {
        'kind': 'EngageNYDomainOrUnit',
        'title': get_text(details),
        'url': make_fully_qualified_url(details['href']),
        'lessons_or_documents': []
    }
    for lesson_or_document in domain_or_unit_li.find('div', class_='tree').find_all('li', attrs={'class': DOCUMENT_OR_LESSON_RE }):
        visit_ela_lesson_or_document(domain_or_unit, lesson_or_document)
    grade_strand_or_module['domains_or_units'].append(domain_or_unit)

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

def visit_ela_lesson_or_document(domain_or_unit, lesson_or_document_li):
    details_div = lesson_or_document_li.find('div', class_='details')
    details = details_div.find('a', attrs={'href': RESOURCE_RE })
    lesson_or_document = {
        'kind': 'EngageNYLessonOrDocument',
        'title': get_text(details),
        'url': make_fully_qualified_url(details['href'])
    }
    domain_or_unit['lessons_or_documents'].append(lesson_or_document)

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
        children={
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

def download_ela_grades(channel_tree, grades):
    for grade in grades:
        download_ela_grade(channel_tree, grade)

def download_ela_grade(channel_tree, grade):
    url = grade['url']
    grade_page = get_parsed_html_from_url(url)
    topic_node = dict(
        kind=content_kinds.TOPIC,
        source_id=url,
        title=grade['title'],
        description=get_description(grade_page),
        children=[]
    )
    for strand_or_module in grade['strands_or_modules']:
        download_ela_strand_or_module(topic_node, strand_or_module)
    channel_tree['children'].append(topic_node)

PDF_RE=compile(r'^/file/.+/(?P<filename>.+\.pdf).*')
def get_pdfs_from_downloadable_resources(resources):
    if not resources:
        return []

    pdfs = resources.find_all('a', attrs={'href': PDF_RE})

    if not pdfs:
        return []

    files = [None] * len(pdfs)

    for i, pdf in enumerate(pdfs):
        url = make_fully_qualified_url(pdf['href'])
        description = get_text(pdf)
        title = strip_byte_size(description)

        files[i] = dict(
            kind=content_kinds.DOCUMENT,
            source_id=url,
            title=title,
            description=description,
            license=ENGAGENY_LICENSE.as_dict(),
            files=[
                dict(
                    file_type=content_kinds.DOCUMENT,
                    path=url,
                )
            ]
        )
    return files

ELA_MODULE_ZIP_FILE_RE = compile(r'^(/file/\d+/download/.*-\w+-pdf.zip).*$') 
def download_ela_strand_or_module(topic, strand_or_module):
    url = strand_or_module['url']
    strand_or_module_page = get_parsed_html_from_url(url)
    strand_or_module_node = dict(
        kind=content_kinds.TOPIC,
        source_id=url,
        title=strand_or_module['title'],
        description=get_description(strand_or_module_page),
        thumbnail=get_thumbnail_url(strand_or_module_page),
        children=[],
    )
    node_children = strand_or_module_node['children']

    # Gather the module's children from zip file
    resources = get_downloadable_resources_section(strand_or_module_page)
    files = []
    if resources:
        module_zip = resources.find('a', attrs={'href': ELA_MODULE_ZIP_FILE_RE})
        if module_zip:
            success, files = download_zip_file(make_fully_qualified_url(module_zip['href']))
            if success:
                module_files = list(filter(lambda filename: MODULE_LEVEL_FILENAME_RE.match(filename) is not None, files))
                children = sorted(
                    map(lambda file_path: get_name_and_dict_from_file_path(file_path), module_files),
                    key=lambda t: t[0]
                )
                children_dict = dict(children)
                overview = children_dict.get('module') or children_dict.get('overview')
                if overview:
                    node_children.append(overview)
                for name, child in children:
                    if name == 'module' or name == 'overview':
                        continue
                    node_children.append(child)
        else:
            node_children.extend(get_pdfs_from_downloadable_resources(resources))

    # Gather the children at the next level down
    for domain_or_unit in strand_or_module['domains_or_units']:
        download_ela_domain_or_unit(strand_or_module_node, domain_or_unit, files)
    topic['children'].append(strand_or_module_node)

def download_ela_domain_or_unit(strand_or_module, domain_or_unit, files):
    url = domain_or_unit['url']
    title = domain_or_unit['title']
    domain_or_unit_page = get_parsed_html_from_url(url)
    domain_or_unit_node = dict(
        kind=content_kinds.TOPIC,
        source_id=url,
        title=title,
        description=get_description(domain_or_unit_page),
        thumbnail=get_thumbnail_url(domain_or_unit_page),
        license=ENGAGENY_LICENSE.as_dict(),
        children=[],
    )
    node_children = domain_or_unit_node['children']

    # Gather the unit's assets
    if files:
        unit_files = list(filter(lambda filename: title in filename, files))
        children = sorted(
            filter(lambda t: t is not None,  map(lambda file_path: get_name_and_dict_from_unit_file_path(file_path), unit_files)),
            key=lambda t:t[0]
        )
        children_dict = dict(children)
        overview = children_dict.get('unit')
        if overview:
            node_children.append(overview)
        for name, child in children:
            if name == 'unit':
                continue
            node_children.append(child)
    else:
        resources = get_downloadable_resources_section(domain_or_unit_page)
        node_children.extend(get_pdfs_from_downloadable_resources(resources))

    for lesson_or_document in domain_or_unit['lessons_or_documents']:
        download_math_lesson(domain_or_unit_node['children'], lesson_or_document)
    strand_or_module['children'].append(domain_or_unit_node)

def download_math_grades(channel_tree, grades):
    for grade in grades:
        download_math_grade(channel_tree, grade)

def get_description(markup_node):
    return get_text(markup_node.find('div', 'content-body'))

def download_math_grade(channel_tree, grade):
    url = grade['url']
    grade_page = get_parsed_html_from_url(url)
    topic_node = dict(
        kind=content_kinds.TOPIC,
        source_id=url,
        title=grade['title'],
        description=get_description(grade_page),
        children=[],
    )
    for mod in grade['modules']:
        download_math_module(topic_node, mod)
    channel_tree['children'].append(topic_node)

def get_thumbnail_url(page):
    thumbnail_url = page.find('img', class_='img-responsive')['src'].split('?')[0]
    return None if get_suffix(thumbnail_url) == '.gif' else thumbnail_url

MODULE_ASSESSMENTS_RE = compile(r'^(?P<segmentsonly>(.)+-as{1,2}es{1,2}ments{0,1}.(zip|pdf))(.)*')
def get_module_assessments(page):
    return page.find_all('a', attrs={ 'href': MODULE_ASSESSMENTS_RE })

MODULE_OVERVIEW_DOCUMENT_RE = compile(r'^(?P<segmentsonly>/file/(.)+-overview(.)*.(pdf|zip))(.)*$')
def get_module_overview_document(page):
    return page.find('a', attrs={'href':  MODULE_OVERVIEW_DOCUMENT_RE })

def download_math_module(topic_node, mod):
    url = mod['url']
    module_page = get_parsed_html_from_url(url)
    description = get_description(module_page)
    module_overview_document_anchor = get_module_overview_document(module_page)
    initial_children = []
    module_overview_full_path = ''
    fetch_overview_bundle = False
    fetch_assessment_bundle = False

    if module_overview_document_anchor is not None:
        module_overview_file = module_overview_document_anchor['href']
        module_overview_full_path = make_fully_qualified_url(module_overview_file)
        thumbnail_url = get_thumbnail_url(module_page)
        overview_node = dict(
            kind=content_kinds.DOCUMENT,
            source_id=url,
            title=mod['title'] + " Overview",
            description=get_description(module_page),
            license=ENGAGENY_LICENSE.as_dict(),
            thumbnail=thumbnail_url,
            files=[
                dict(
                    file_type=content_kinds.DOCUMENT,
                    path=module_overview_full_path
                ),
            ]
        )
        if get_suffix(module_overview_full_path) == ".pdf":
            initial_children.append(overview_node)
        else:
            fetch_overview_bundle = True
    else:
        print("didn't find a math module overview match")

    module_assessment_anchors = get_module_assessments(module_page)
    if module_assessment_anchors:
        for module_assessment_anchor in module_assessment_anchors:
            module_assessment_file = module_assessment_anchor['href']
            module_assessment_full_path = make_fully_qualified_url(module_assessment_file)
            file_extension = get_suffix(module_assessment_full_path)
            if file_extension == ".pdf":
                assessment_node = dict(
                    kind=content_kinds.DOCUMENT,
                    source_id=module_assessment_full_path,
                    title=strip_byte_size(get_text(module_assessment_anchor)),
                    description=module_assessment_anchor['title'],
                    license=ENGAGENY_LICENSE.as_dict(),
                    files=[
                        dict(
                            file_type=content_kinds.DOCUMENT,
                            path=module_assessment_full_path
                    )
                ])
                initial_children.append(assessment_node)
            else:
                fetch_assessment_bundle = True
    else:
        print("didn't find a math module assessment(s) match")

    if fetch_assessment_bundle:
        print('will fetch assessment bundle:', module_assessment_full_path)
        success, files = download_zip_file(module_assessment_full_path)
        if success and files:
            files.reverse()
            for i, file in enumerate(files):
                if fetch_overview_bundle and get_suffix(module_overview_full_path) == '.pdf' and file.endswith('overview.pdf'):
                    continue
                title = get_item_from_bundle_title(file)
                initial_children.append(dict(
                    kind=content_kinds.DOCUMENT,
                    source_id=file,
                    title=title,
                    description=title,
                    license=ENGAGENY_LICENSE.as_dict(),
                    files=[
                        dict(
                            file_type=content_kinds.DOCUMENT,
                            path=file
                        )
                    ]
                ))
        else:
            print(success, 'download zip file for', module_assessment_full_path)

    module_node = dict(
        kind=content_kinds.TOPIC,
        source_id=url,
        title=mod['title'],
        description=description,
        children=initial_children,
    )
    download_math_topics(module_node, mod['topics'])
    topic_node['children'].append(module_node)

def download_math_topics(module_node, topics):
    for topic in topics:
        download_math_topic(module_node, topic)

def download_math_topic(module_node, topic):
    initial_children = []
    url = topic['url']
    topic_page = get_parsed_html_from_url(url)
    description = get_description(topic_page)

    topic_overview_anchor = get_module_overview_document(topic_page)
    if topic_overview_anchor is not None:
        overview_document_file = topic_overview_anchor['href']
        document_url = make_fully_qualified_url(overview_document_file)
        overview_node = dict(
            kind=content_kinds.DOCUMENT,
            source_id=document_url,
            title=topic['title'] + ' Overview',
            description=description,
            license=ENGAGENY_LICENSE.as_dict(),
            thumbnail=get_thumbnail_url(topic_page),
            files=[
                dict(
                    file_type=content_kinds.DOCUMENT,
                    path=document_url,
                )
            ]
        )
        initial_children.append(overview_node)

    download_math_lessons(initial_children, topic['lessons'])

    topic_node = dict(
        kind=content_kinds.TOPIC,
        source_id=url,
        title=topic['title'],
        description=description,
        children=initial_children
    )
    module_node['children'].append(topic_node)

def download_math_lessons(parent, lessons):
    for lesson in lessons:
        download_math_lesson(parent, lesson)

def get_downloadable_resources_section(page):
    return page.find('div', class_='pane-downloadable-resources')

def get_related_resources_section(page):
    return page.find('div', class_='pane-related-items')

def download_math_lesson(parent, lesson):
    lesson_url = lesson['url']
    lesson_page = get_parsed_html_from_url(lesson_url)
    title = lesson['title']
    description = get_description(lesson_page)
    lesson_data = dict(
        kind=content_kinds.TOPIC,
        source_id=lesson_url,
        title=title,
        description=description,
        language='en',
        thumbnail=get_thumbnail_url(lesson_page),
        children=[],
    )
    resources_pane = get_downloadable_resources_section(lesson_page)

    if resources_pane is None:
        return

    resources_table = resources_pane.find('table')
    resources_rows = resources_table.find_all('tr')

    for row in resources_rows:
        doc_link = row.find_all('td')[1].find('a')
        description = get_text(doc_link)
        title = strip_byte_size(description)
        sanitized_doc_link = doc_link['href'].split('?')[0]
        doc_path = make_fully_qualified_url(sanitized_doc_link)
        if 'pdf' in doc_path:
            document_node = dict(
                kind=content_kinds.DOCUMENT,
                source_id=lesson_url + ":" + sanitized_doc_link,
                title=title,
                author='Engage NY',
                description=description,
                license=ENGAGENY_LICENSE.as_dict(),
                thumbnail=None,
                files=[dict(
                    file_type=content_kinds.DOCUMENT,
                    path=doc_path,
                    language='en',
                )],
                language='en',
            )
            lesson_data['children'].append(document_node)

    parent.append(lesson_data)

def build_scraping_json_tree(web_resource_tree):
    channel_tree = dict(
        source_domain='engageny.org',
        source_id='engageny',
        title=web_resource_tree['title'],
        description='EngageNY Common Core Curriculum Content... ELA and CCSSM combined',
        language=web_resource_tree['language'],
        thumbnail='./content/engageny_logo.png',
        children=[],
    )
    download_ela_grades(channel_tree, web_resource_tree['children']['ela']['grades'])
    download_math_grades(channel_tree, web_resource_tree['children']['math']['grades'])
    return channel_tree

def scraping_part(json_tree_path):
    """
    Download all categories, subpages, modules, and resources from engageny and
    store them as a ricecooker json tree in the file `json_tree_path`.
    """
    # Read web_resource_trees.json
    with open(os.path.join(TREES_DATA_DIR, CRAWLING_STAGE_OUTPUT)) as json_file:
        web_resource_tree = json.load(json_file)
        assert web_resource_tree['kind'] == 'EngageNYWebResourceTree'

    # Build a Ricecooker tree from scraping process
    ricecooker_json_tree = build_scraping_json_tree(web_resource_tree)
    LOGGER.info('Finished building ricecooker_json_tree')

    # Write out ricecooker_json_tree.json
    write_tree_to_json_tree(os.path.join(TREES_DATA_DIR, SCRAPING_STAGE_OUTPUT) , ricecooker_json_tree)



# CHEF
################################################################################

class EngageNYChef(JsonTreeChef):
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
        Build the ricecooker_json_tree that will create the ricecooker channel tree.
        """
        kwargs = {}     # combined dictionary of argparse args and extra options
        kwargs.update(args)
        kwargs.update(options)
        json_tree_path = self.get_json_tree_path(**kwargs)
        scraping_part(json_tree_path)


    def pre_run(self, args, options):
        """
        Run the preliminary parts.
        """
        self.crawl(args, options)
        self.scrape(args, options)


    def get_json_tree_path(self, **kwargs):
        """
        Return path to the ricecooker json tree file.
        Parent class `JsonTreeChef` implements get_channel and construct_channel
        that read their data from the json file specified by this function.
        Currently there is a single json file SCRAPING_STAGE_OUTPUT, but maybe in
        the future this function can point to different files depending on the
        kwarg `lang` (that's how it's done in several other mulitilingual chefs).
        """
        json_tree_path = os.path.join(TREES_DATA_DIR, SCRAPING_STAGE_OUTPUT)
        return json_tree_path




# CLI
################################################################################

if __name__ == '__main__':
    chef = EngageNYChef()
    chef.main()
