#!/usr/bin/env python

# region Imports

import translation

import json
import logging
import os
import re
from sys import exit, exc_info
from time import sleep

from bs4 import BeautifulSoup
import requests

from collections import defaultdict
from re import compile
import zipfile
import io
import argparse
from google.api.core import exceptions

from le_utils.constants import content_kinds, licenses
from le_utils.constants.languages import getlang
from ricecooker.chefs import JsonTreeChef
from ricecooker.classes.licenses import get_license
from ricecooker.utils.caching import CacheForeverHeuristic, FileCache, CacheControlAdapter
from ricecooker.utils.jsontrees import write_tree_to_json_tree

from pathlib import PurePosixPath
# endregion Imports


def create_http_session(hostname):
    sess = requests.Session()
    cache = FileCache('.webcache')
    basic_adapter = CacheControlAdapter(cache=cache)
    forever_adapter = CacheControlAdapter(heuristic=CacheForeverHeuristic(), cache=cache)
    sess.mount('http://', basic_adapter)
    sess.mount('https://', basic_adapter)
    sess.mount('http://www.' + hostname, forever_adapter)
    sess.mount('https://www.' + hostname, forever_adapter)
    return sess


def create_logger():
    logging.getLogger("cachecontrol.controller").setLevel(logging.WARNING)
    logging.getLogger("requests.packages").setLevel(logging.WARNING)
    from ricecooker.config import LOGGER
    LOGGER.setLevel(logging.DEBUG)
    return LOGGER

# region Chef


class EngageNYChef(JsonTreeChef):
    """
    This class takes care of downloading resources from engageny.org and uploading
    them to Kolibri Studio, the content curation server.
    """
    HOSTNAME = 'engageny.org'
    ENGAGENY_CC_START_URL = f'https://www.{HOSTNAME}/common-core-curriculum'
    ENGAGENY_LICENSE = get_license(licenses.CC_BY_NC_SA, copyright_holder='Engage NY').as_dict()

    DATA_DIR = 'chefdata'
    TREES_DATA_DIR = os.path.join(DATA_DIR, 'trees')
    PDFS_DATA_DIR = os.path.join(DATA_DIR, 'pdfs')
    CRAWLING_STAGE_OUTPUT = 'web_resource_tree.json'
    SCRAPING_STAGE_OUTPUT = 'ricecooker_json_tree'

    def __init__(self, http_session, logger):
        super(EngageNYChef, self).__init__()
        self.arg_parser = argparse.ArgumentParser(
            description="EngageNY sushi chef.",
            add_help=True,
            parents=[self.arg_parser],
        )
        self._http_session = http_session
        self._logger = logger
        self._lang = None

        self.SUPPORTED_LANGUAGES = {
            lang_code: getlang(lang_code)
            for lang_code in ['ar', 'bn', 'en', 'es', 'ht', 'zh-CN', 'zh-TW']
        }
        self.EN_DOWNLOADABLE_RESOURCE_RE = re.compile('\.pdf|topic-\w+-lessons-\d+-\d+\.zip', re.I)

        self.NON_EN_DOWNLOADABLE_RESOURCE_RES = {
            lang_code: re.compile('({}).+pdf\.zip'.format(self.fixup_language_name(lang.name)), re.I)
            for lang_code, lang in self.SUPPORTED_LANGUAGES.items() if lang_code is not 'en'
        }

    # region Helper functions

    @staticmethod
    def get_text(x):
        return "" if x is None else x.get_text().replace('\r', '').replace('\n', ' ').strip()

    STRIP_BYTESIZE_RE = compile(r'^(.*)\s+\((\d+|\d+\.\d+)\s+\w+B\)')

    @staticmethod
    def strip_byte_size(s):
        m = EngageNYChef.STRIP_BYTESIZE_RE.match(s)
        if not m:
            raise Exception('STRIP_BYTESIZE_RE did not match')
        return m.group(1)

    @staticmethod
    def get_suffix(path):
        return PurePosixPath(path).suffix

    MODULE_LEVEL_PDF_INDIVIDUAL_FILES_RE = compile(r'.+/.+/PDF\s+Individual\s+Files/ela-\w(\d+)-(\w)(\d+)-(\w+-\w+).pdf')

    MODULE_LEVEL_FILENAME_RE = compile(r'^.+/.+/.+/(?:Module\sLevel\sDocuments/){0,1}(?P<grade>\d+)(?P<moduleletter>\w)(?P<modulenumber>\w+)\.(?P<name>\D+)\.pdf$')

    MODULE_EXTENSION_FILENAME_RE = compile(r'^[^/]+/[^/]+/[^/]+/(?:(?P<subdir>[^/]+)/){0,1}ela-grade-(?P<grade>\d+)[-\.]ext[-\.](?P<name>.+).pdf$')

    LESSON_RE = compile(r'^(?P<lesson>[^\d]+)(?P<number>\d+)$')

    def get_name_and_dict_from_file_path(self, file_path):
        def get_title_and_name(m):
            grade, module_letter, module_number, name = m.groups()
            title = ['grade', grade]
            if module_letter == 'm':
                title.extend(['module', module_number])
            if name == 'module':
                title.append('overview')
            else:
                title.extend(name.replace('module', '').replace('-', ' ').split())
            return ' '.join(map(str, title)).title(), name

        def get_module_extension_title_and_name(m):
            subdir, grade, name = m.groups()
            title = ['grade', grade, 'extension', 'module']
            if subdir:
                title.append(subdir)
            lesson_m = EngageNYChef.LESSON_RE.match(name)
            if lesson_m:
                title.append(lesson_m.group('number'))
            else:
                title.extend(name.replace("module", "").replace("-", " ").split())
            return ' '.join(map(str, title)).title(), name

        m = EngageNYChef.MODULE_LEVEL_FILENAME_RE.match(file_path) or EngageNYChef.MODULE_LEVEL_PDF_INDIVIDUAL_FILES_RE.match(file_path)
        if m:
            title, name = get_title_and_name(m)
        else:
            m = EngageNYChef.MODULE_EXTENSION_FILENAME_RE.match(file_path)
            if not m:
                raise Exception('Neither MODULE_LEVEL_FILENAME_RE or MODULE_LEVEL_PDF_INDIVIDUAL_FILES_RE or MODULE_EXTENSION_FILENAME_RE could match')
            title, name = get_module_extension_title_and_name(m)
        translated_title = self._(title)
        return name.lower(), dict(
            kind=content_kinds.DOCUMENT,
            source_id=os.path.basename(file_path),
            title=translated_title,
            description=translated_title,
            license=EngageNYChef.ENGAGENY_LICENSE,
            files=[
                dict(
                    file_type=content_kinds.DOCUMENT,
                    path=file_path
                )
            ],
        )

    UNIT_LEVEL_FILENAME_RE = compile(r'^.*(?P<grade>\d+)(?P<moduleletter>\w+)(?P<modulenumber>\d+)\.(?P<unitnumber>\d+)(?P<name>\D+)\.pdf$')

    def get_name_and_dict_from_unit_file_path(self, file_path):
        m = EngageNYChef.UNIT_LEVEL_FILENAME_RE.match(file_path)
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

        translated_title = self._(title.title())
        return name.lower(), dict(
            kind=content_kinds.DOCUMENT,
            source_id=os.path.basename(file_path),
            title=translated_title,
            description=translated_title,
            license=EngageNYChef.ENGAGENY_LICENSE,
            files=[
                dict(
                    file_type=content_kinds.DOCUMENT,
                    path=file_path
                )
            ],
        )

    ITEM_FROM_BUNDLE_RE = compile(r'^.+/(?P<area>.+(-i+){0,1})-(?P<grade>.+)-(?P<module>.+)-(?P<assessment_cutoff>.+-){0,1}(?P<level>.+)-(?P<type>.+)\..+$')

    @staticmethod
    def get_item_from_bundle_title(path):
        m = EngageNYChef.ITEM_FROM_BUNDLE_RE.match(path)
        if m:
            return ' '.join(filter(lambda x: x is not None, m.groups())).title()
        raise Exception('Regex to match bundle item filename did not match')

    def get_parsed_html_from_url(self, url, *args, **kwargs):
        response = self._http_session.get(url, *args, **kwargs)
        if response.status_code != 200:
            self._logger.error("STATUS: {}, URL: {}", response.status_code, url)
        elif not response.from_cache:
            self._logger.debug("NOT CACHED:", url)
        return BeautifulSoup(response.content, "html.parser")

    def download_zip_file(self, url):
        if not url:
            return False, None

        if EngageNYChef.get_suffix(url) != '.zip':
            return False, None

        response = self._http_session.get(url)
        if response.status_code != 200:
            self._logger.error("STATUS: {}, URL: {}", response.status_code, url)
            return False, None
        elif not response.from_cache:
            self._logger.debug("NOT CACHED:", url)

        archive = zipfile.ZipFile(io.BytesIO(response.content))
        archive_members = list(filter(lambda f: f.filename.endswith('.pdf'), archive.infolist()))
        archive_member_names = [None] * len(archive_members)
        for i, pdf in enumerate(archive_members):
            path = os.path.join(EngageNYChef.PDFS_DATA_DIR, pdf.filename)
            archive_member_names[i] = path
            if not os.path.exists(path):
                archive.extract(pdf, EngageNYChef.PDFS_DATA_DIR)
        return True, archive_member_names

    @staticmethod
    def strip_token(url):
        return url.split('?')[0]

    @staticmethod
    def make_fully_qualified_url(url):
        if url.startswith("//"):
            print('unexpected // url', url)
            return EngageNYChef.strip_token("https:" + url)
        elif url.startswith("/"):
            return EngageNYChef.strip_token("https://www.engageny.org" + url)
        return EngageNYChef.strip_token(url)

    # endregion Helper functions

    # region Crawling
    def crawl(self, args, options):
        """
        PART 1: crawling
        Builds the json web resource tree --- the recipe of what is to be downloaded.
        """
        doc = self.get_parsed_html_from_url(EngageNYChef.ENGAGENY_CC_START_URL)
        dual_toc_div = doc.find('div', id='mini-panel-common_core_curriculum')
        ela_toc = dual_toc_div.find('div', class_='panel-col-first')
        math_toc = dual_toc_div.find('div', class_='panel-col-last')
        ela_hierarchy, math_hierarchy = self._crawl_grades(ela_toc, math_toc)
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
        json_file_name = os.path.join(EngageNYChef.TREES_DATA_DIR, EngageNYChef.CRAWLING_STAGE_OUTPUT)
        with open(json_file_name, 'w') as json_file:
            json.dump(web_resource_tree, json_file, indent=2)
            self._logger.info('Crawling results stored in ' + json_file_name)
        return web_resource_tree

    def _crawl_grades(self, ela_toc, math_toc):
        ela_grades = EngageNYChef._crawl_toc_grades(ela_toc, children_label='strands_or_modules')
        math_grades = EngageNYChef._crawl_toc_grades(math_toc)
        for grade in ela_grades:
            self._crawl_ela_grade(grade)
        for grade in math_grades:
            self._crawl_math_grade(grade)
        return ela_grades, math_grades

    CONTENT_OR_RESOURCE_URL_RE = compile(r'/(content|resource)/*')

    @staticmethod
    def _crawl_toc_grades(toc, children_label='modules'):
        grades = []
        for grade in toc.find_all('a', attrs={'href': EngageNYChef.CONTENT_OR_RESOURCE_URL_RE }):
            grade_path = grade['href']
            grade_url = EngageNYChef.make_fully_qualified_url(grade_path)
            grades.append({
                'kind': 'EngageNYGrade',
                'url': grade_url,
                'title': EngageNYChef.get_text(grade),
                children_label: []
            })
        return grades

    STRAND_OR_MODULE_RE = compile('\w*\s*(strand|module)\s*\w*')

    def _crawl_ela_grade(self, grade):
        grade_page = self.get_parsed_html_from_url(grade['url'])
        grade_curriculum_toc = grade_page.find('div', class_='nysed-book-outline curriculum-map')
        for strand_or_module_li in grade_curriculum_toc.find_all('li', attrs={'class': EngageNYChef.STRAND_OR_MODULE_RE}):
            self._crawl_ela_strand_or_module(grade, strand_or_module_li)

    MODULE_URL_RE = compile(r'^/resource/(.)+-module-(\d)+$')

    def _crawl_math_grade(self, grade):
        grade_page = self.get_parsed_html_from_url(grade['url'])
        grade_curriculum_toc = grade_page.find('div', class_='nysed-book-outline curriculum-map')
        for module_li in grade_curriculum_toc.find_all('li', class_='module'):
            self._crawl_math_module(grade, module_li)

    def _crawl_math_module(self, grade, module_li):
        details_div = module_li.find('div', class_='details')
        details = details_div.find('a', attrs={'href': EngageNYChef.MODULE_URL_RE })
        grade_module = {
            'kind': 'EngageNYModule',
            'title': EngageNYChef.get_text(details),
            'url': EngageNYChef.make_fully_qualified_url(details['href']),
            'topics': [],
        }
        for topic_li in module_li.find('div', class_='tree').find_all('li', class_='topic'):
            EngageNYChef._crawl_math_topic(grade_module['topics'], topic_li)
        grade['modules'].append(grade_module)

    RESOURCE_RE = compile(r'^/resource')
    DOMAIN_OR_UNIT_RE = compile(r'\w*\s*(domain|unit)\s*\w*')

    def _crawl_ela_strand_or_module(self, grade, strand_or_module_li):
        details_div = strand_or_module_li.find('div', class_='details')
        details = details_div.find('a',  attrs={'href': EngageNYChef.RESOURCE_RE})
        grade_strand_or_module = {
            'kind': 'EngageNYStrandOrModule',
            'title': EngageNYChef.get_text(details),
            'url': EngageNYChef.make_fully_qualified_url(details['href']),
            'domains_or_units': []
        }
        for domain_or_unit in strand_or_module_li.find('div', class_='tree').find_all('li', attrs={'class': EngageNYChef.DOMAIN_OR_UNIT_RE}):
            EngageNYChef._crawl_ela_domain_or_unit(grade_strand_or_module, domain_or_unit)
        grade['strands_or_modules'].append(grade_strand_or_module)

    TOPIC_URL_RE = compile(r'^(.)+-topic(.)*')

    @staticmethod
    def _crawl_math_topic(topics, topic_li):
        details_div = topic_li.find('div', class_='details')
        details = details_div.find('a', attrs={'href': EngageNYChef.TOPIC_URL_RE })
        topic = {
            'kind': 'EngageNYTopic',
            'title': EngageNYChef.get_text(details),
            'url': EngageNYChef.make_fully_qualified_url(details['href']),
            'lessons': [],
        }
        for lesson_li in topic_li.find('div', class_='tree').find_all('li', class_='lesson'):
            EngageNYChef._crawl_math_lesson(topic, lesson_li)
        topics.append(topic)

    DOCUMENT_OR_LESSON_RE = compile(r'\w*\s*(document|lesson)\w*\s*')

    @staticmethod
    def _crawl_ela_domain_or_unit(grade_strand_or_module, domain_or_unit_li):
        details_div = domain_or_unit_li.find('div', class_='details')
        details = details_div.find('a', attrs={'href': EngageNYChef.RESOURCE_RE })
        domain_or_unit = {
            'kind': 'EngageNYDomainOrUnit',
            'title': EngageNYChef.get_text(details),
            'url': EngageNYChef.make_fully_qualified_url(details['href']),
            'lessons_or_documents': []
        }
        for lesson_or_document in domain_or_unit_li.find('div', class_='tree').find_all('li', attrs={'class': EngageNYChef.DOCUMENT_OR_LESSON_RE }):
            EngageNYChef._crawl_ela_lesson_or_document(domain_or_unit, lesson_or_document)
        grade_strand_or_module['domains_or_units'].append(domain_or_unit)

    LESSON_URL_RE = compile(r'^(.)+-lesson(.)*')

    @staticmethod
    def _crawl_math_lesson(topic, lesson_li):
        details_div = lesson_li.find('div', class_='details')
        details = details_div.find('a', attrs={'href': EngageNYChef.LESSON_URL_RE})
        lesson = {
            'kind': 'EngageNYLesson',
            'title': EngageNYChef.get_text(details),
            'url': EngageNYChef.make_fully_qualified_url(details['href'])
        }
        topic['lessons'].append(lesson)

    @staticmethod
    def _crawl_ela_lesson_or_document(domain_or_unit, lesson_or_document_li):
        details_div = lesson_or_document_li.find('div', class_='details')
        details = details_div.find('a', attrs={'href': EngageNYChef.RESOURCE_RE })
        lesson_or_document = {
            'kind': 'EngageNYLessonOrDocument',
            'title': EngageNYChef.get_text(details),
            'url': EngageNYChef.make_fully_qualified_url(details['href'])
        }
        domain_or_unit['lessons_or_documents'].append(lesson_or_document)

    # endregion Crawling

    # region Scraping
    def _(self, msg):
        sleep_period_secs = 100
        max_tries = 4
        msg_length = len(msg)
        msg_to_translate = ''
        if msg_length >= 5000:
            self._logger.warn(f'Message is longer ({msg_length}) than Google Translation API limit `5000`, we might consider chunking the translation')
            msg_to_translate = msg[:4996] + " ..."
        else:
            msg_to_translate = msg

        try_ = 1
        while try_ < max_tries:
            try:
                response = self.translation_client.translate(msg_to_translate)
                if isinstance(response, list):
                    return response[0]['translatedText']
                return response['translatedText']
            except exceptions.Forbidden as forbidden:
                self._logger.warn(f'Error `{forbidden}`, the message will not be translated')
                return msg
            except:
                e = exc_info()[0]
                self._logger.warn(f'An error occurred `{e}`, will sleep for {sleep_period_secs} seconds, try `{try_}` out of {max_tries}')
                try_ += 1
                sleep(sleep_period_secs)
        self._logger.warn('All retries exahusted, the message will not be translated')
        return msg

    def scrape(self, args, options):
        """
        PART 2: SCRAPING
        Build the ricecooker_json_tree that will create the ricecooker channel tree.
        """
        kwargs = {}     # combined dictionary of argparse args and extra options
        kwargs.update(args)
        kwargs.update(options)
        json_tree_path = self.get_json_tree_path(**kwargs)
        self._scraping_part(json_tree_path, kwargs)

    def get_json_tree_path(self, **kwargs):
        """
        Return path to the ricecooker json tree file.
        Parent class `JsonTreeChef` implements get_channel and construct_channel
        that read their data from the json file specified by this function.
        Currently there is a single json file SCRAPING_STAGE_OUTPUT, but maybe in
        the future this function can point to different files depending on the
        kwarg `lang` (that's how it's done in several other mulitilingual chefs).
        """
        base_path = os.path.join(EngageNYChef.TREES_DATA_DIR, EngageNYChef.SCRAPING_STAGE_OUTPUT)
        json_tree_path = f'{base_path}_{self._lang}.json'
        self._logger.info('json_tree_path', json_tree_path)
        return json_tree_path

    def _scrape_ela_grades(self, channel_tree, grades):
        for grade in grades:
            self._scrape_ela_grade(channel_tree, grade)

    def _scrape_ela_grade(self, channel_tree, grade):
        url = grade['url']
        grade_page = self.get_parsed_html_from_url(url)
        topic_node = dict(
            kind=content_kinds.TOPIC,
            source_id=url,
            title=self._(grade['title']),
            description=self._(EngageNYChef._get_description(grade_page)),
            children=[]
        )
        for strand_or_module in grade['strands_or_modules']:
            self._scrape_ela_strand_or_module(topic_node, strand_or_module)
        channel_tree['children'].append(topic_node)

    PDF_RE = compile(r'^/file/.+/(?P<filename>.+\.pdf).*')

    def _get_pdfs_from_downloadable_resources(self, resources):
        if not resources:
            return []
        pdfs = resources.find_all('a', attrs={'href': EngageNYChef.PDF_RE})
        if not pdfs:
            return []
        files = [None] * len(pdfs)
        for i, pdf in enumerate(pdfs):
            url = EngageNYChef.make_fully_qualified_url(pdf['href'])
            description = EngageNYChef.get_text(pdf)
            title = EngageNYChef.strip_byte_size(description)
            files[i] = dict(
                kind=content_kinds.DOCUMENT,
                source_id=os.path.basename(url),
                title=self._(title),
                description=self._(description),
                license=EngageNYChef.ENGAGENY_LICENSE,
                files=[
                    dict(
                        file_type=content_kinds.DOCUMENT,
                        path=url,
                    )
                ]
            )
        return files

    ELA_MODULE_ZIP_FILE_RE = compile(r'^(/file/\d+/download/.*-\w+-pdf.zip).*$')

    def _scrape_ela_strand_or_module(self, topic, strand_or_module):
        url = strand_or_module['url']
        strand_or_module_page = self.get_parsed_html_from_url(url)
        strand_or_module_node = dict(
            kind=content_kinds.TOPIC,
            source_id=url,
            title=self._(strand_or_module['title']),
            description=self._(EngageNYChef._get_description(strand_or_module_page)),
            thumbnail=EngageNYChef._get_thumbnail_url(strand_or_module_page),
            children=[],
        )
        node_children = strand_or_module_node['children']

        # Gather the module's children from zip file
        resources = EngageNYChef._get_downloadable_resources_section(strand_or_module_page)
        files = []
        if resources:
            module_zip = resources.find('a', attrs={'href': EngageNYChef.ELA_MODULE_ZIP_FILE_RE})
            if module_zip:
                success, files = self.download_zip_file(EngageNYChef.make_fully_qualified_url(module_zip['href']))
                if success:
                    module_files = list(filter(lambda filename: EngageNYChef.MODULE_LEVEL_FILENAME_RE.match(filename) is not None or EngageNYChef.MODULE_LEVEL_PDF_INDIVIDUAL_FILES_RE.match(filename) is not None or EngageNYChef.MODULE_EXTENSION_FILENAME_RE.match(filename) is not None, files))
                    children = sorted(
                        map(lambda file_path: self.get_name_and_dict_from_file_path(file_path), module_files),
                        key=lambda t: t[0]
                    )
                    children_dict = dict(children)
                    overview = children_dict.get('module') or children_dict.get('overview') or children_dict.get('module-overview')
                    if overview:
                        node_children.append(overview)
                    for name, child in children:
                        if name == 'module' or name == 'overview' or name == 'module-overview':
                            continue
                        node_children.append(child)
            else:
                node_children.extend(self._get_pdfs_from_downloadable_resources(resources))
        # Gather the children at the next level down
        for domain_or_unit in strand_or_module['domains_or_units']:
            self._scrape_ela_domain_or_unit(strand_or_module_node, domain_or_unit, files)
        topic['children'].append(strand_or_module_node)

    def _scrape_ela_domain_or_unit(self, strand_or_module, domain_or_unit, files):
        url = domain_or_unit['url']
        title = domain_or_unit['title']
        domain_or_unit_page = self.get_parsed_html_from_url(url)
        domain_or_unit_node = dict(
            kind=content_kinds.TOPIC,
            source_id=url,
            title=self._(title),
            description=self._(EngageNYChef._get_description(domain_or_unit_page)),
            thumbnail=EngageNYChef._get_thumbnail_url(domain_or_unit_page),
            license=EngageNYChef.ENGAGENY_LICENSE,
            children=[],
        )
        node_children = domain_or_unit_node['children']

        # Gather the unit's assets
        if files:
            unit_files = list(filter(lambda filename: title in filename, files))
            children = sorted(
                filter(lambda t: t is not None,  map(lambda file_path: self.get_name_and_dict_from_unit_file_path(file_path), unit_files)),
                key=lambda t: t[0]
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
            resources = EngageNYChef._get_downloadable_resources_section(domain_or_unit_page)
            node_children.extend(self._get_pdfs_from_downloadable_resources(resources))

        for lesson_or_document in domain_or_unit['lessons_or_documents']:
            self._scrape_math_lesson(domain_or_unit_node['children'], lesson_or_document, lambda _: _)
        strand_or_module['children'].append(domain_or_unit_node)

    def _scrape_math_grades(self, channel_tree, grades):
        for grade in grades:
            self._scrape_math_grade(channel_tree, grade)

    @staticmethod
    def _get_description(markup_node):
        return EngageNYChef.get_text(markup_node.find('div', 'content-body'))

    def _scrape_math_grade(self, channel_tree, grade):
        url = grade['url']
        grade_page = self.get_parsed_html_from_url(url)
        topic_node = dict(
            kind=content_kinds.TOPIC,
            source_id=url,
            title=self._(grade['title']),
            description=self._(EngageNYChef._get_description(grade_page)),
            children=[],
        )
        for mod in grade['modules']:
            self._scrape_math_module(topic_node, mod)
        channel_tree['children'].append(topic_node)

    @staticmethod
    def _get_thumbnail_url(page):
        thumbnail_url = page.find('img', class_='img-responsive')['src'].split('?')[0] or page.find('meta', property='og:image')['content']
        return None if EngageNYChef.get_suffix(thumbnail_url) == '.gif' else thumbnail_url

    MODULE_ASSESSMENTS_RE = compile(r'^(?P<segmentsonly>(.)+-as{1,2}es{1,2}ments{0,1}.(zip|pdf))(.)*')

    @staticmethod
    def _get_module_assessments(page):
        return page.find_all('a', attrs={'href': EngageNYChef.MODULE_ASSESSMENTS_RE})

    MODULE_OVERVIEW_DOCUMENT_RE = compile(r'^(?P<segmentsonly>/file/(.)+-overview(.)*.(pdf|zip))(.)*$')

    @staticmethod
    def _get_module_overview_document(page):
        return page.find('a', attrs={'href':  EngageNYChef.MODULE_OVERVIEW_DOCUMENT_RE})

    def groupby(self, key, seq):
        d = defaultdict(list)
        for item in seq:
            d[key(item)].append(item)
        return d

    def uniques(self, seq, key=None):
        seen = set()
        return [item for item in seq if (key(item) if key else item) not in seen and not seen.add(key(item) if key else item)]

    # FIXME: Haitian-Creole is coming across as Creole-Haitian which won't match anything,
    # since there are currently no Haitian translated docs, that's okay,
    # but once they are available they will be ignored
    def fixup_language_name(self, name):
        clean = name.replace('Castilian', '').replace(',', '').replace(';', '')
        unique_values = self.uniques(clean.split())
        return '\-'.join(reversed(unique_values))

    def _scrape_math_module(self, topic_node, mod):
        def file_extension(_):
            return _.split('.')[-1].lower()

        # TODO: Figure out a way to set the regex for `en` to
        # EN_DOWNLOADABLE_RESOURCE_RE at the time we construct NON_EN_DOWNLOADABLE_RESOURCE_RES
        downloadable_resources_re = self.EN_DOWNLOADABLE_RESOURCE_RE if self._lang == 'en' else self.NON_EN_DOWNLOADABLE_RESOURCE_RES[self._lang]
        url = mod['url']
        module_page = self.get_parsed_html_from_url(url)
        resources = EngageNYChef._get_downloadable_resources_section(module_page)
        anchors = resources.find_all('a', attrs={'href': downloadable_resources_re})
        filenames = [EngageNYChef.strip_token(a['href']) for a in anchors]
        files_by_extension = self.groupby(file_extension, filenames)

        zip_files = files_by_extension.get('zip')
        all_pdf_files = list(map(EngageNYChef.make_fully_qualified_url, files_by_extension.get('pdf', [])))
        if zip_files:
            for f in zip_files:
                success, files = self.download_zip_file(EngageNYChef.make_fully_qualified_url(f))
                if success:
                    all_pdf_files.extend(files)

        unique_files = self.uniques(all_pdf_files, os.path.basename)
        module_node = dict(
            kind=content_kinds.TOPIC,
            source_id=url,
            title=self._(mod['title']),
            description=EngageNYChef._get_description(module_page),
            # TODO: Sort correctly
            children=[self._get_document(f) for f in unique_files],
            language=self._lang,
        )
        self._scrape_math_topics(module_node, mod['topics'], self._location_resolver({os.path.basename(f): f for f in unique_files}))
        topic_node['children'].append(module_node)
        return unique_files

    def _get_document(self, f):
        filename = os.path.basename(f)
        sanitized_filename = self._(filename.split('.')[0].replace('-', ' ').title())
        return dict(
            kind=content_kinds.DOCUMENT,
            source_id=filename,
            title=sanitized_filename,
            description=sanitized_filename,
            license=EngageNYChef.ENGAGENY_LICENSE,
            language=self._lang,
            files=[
                dict(
                    file_type=content_kinds.DOCUMENT,
                    path=f
                )
            ]
        )

    def _location_resolver(self, env):
        def resolve(name):
            filename = os.path.basename(name)
            return env.get(filename, name)
        return resolve

    SUPPORTED_TRANSLATIONS_RE = compile(r'(Spanish|Simplified-Chinese|Traditional-Chinese|Arabic|Bengali|Haitian-Creole)-pdf.zip', re.I)

    @staticmethod
    def _get_translations(module_page):
        downloadable_resources = EngageNYChef._get_downloadable_resources_section(module_page)
        return [
            (
                EngageNYChef.SUPPORTED_TRANSLATIONS_RE.search(t['href']).group(1).replace('-', ' '),
                EngageNYChef.make_fully_qualified_url(t['href'])
            )
            for t in
            downloadable_resources.find_all('a', attrs={'href': EngageNYChef.SUPPORTED_TRANSLATIONS_RE})
        ]

    def _scrape_math_topics(self, module_node, topics, asset_resolver):
        for topic in topics:
            self._scrape_math_topic(module_node, topic, asset_resolver)

    def _scrape_math_topic(self, module_node, topic, asset_resolver):
        initial_children = []
        url = topic['url']
        topic_page = self.get_parsed_html_from_url(url)
        description = EngageNYChef._get_description(topic_page)

        topic_overview_anchor = EngageNYChef._get_module_overview_document(topic_page)
        if topic_overview_anchor is not None:
            overview_document_file = topic_overview_anchor['href']
            document_url = EngageNYChef.make_fully_qualified_url(overview_document_file)
            overview_node = dict(
                kind=content_kinds.DOCUMENT,
                source_id=os.path.basename(document_url),
                title=self._(topic['title'] + ' Overview'),
                description=self._(description),
                license=EngageNYChef.ENGAGENY_LICENSE,
                thumbnail=EngageNYChef._get_thumbnail_url(topic_page),
                files=[
                    dict(
                        file_type=content_kinds.DOCUMENT,
                        path=asset_resolver(document_url),
                    )
                ]
            )
            initial_children.append(overview_node)

        self._scrape_math_lessons(initial_children, topic['lessons'], asset_resolver)

        topic_node = dict(
            kind=content_kinds.TOPIC,
            source_id=url,
            title=self._(topic['title']),
            description=self._(description),
            children=initial_children
        )
        module_node['children'].append(topic_node)

    def _scrape_math_lessons(self, parent, lessons, asset_resolver):
        for lesson in lessons:
            self._scrape_math_lesson(parent, lesson, asset_resolver)

    @staticmethod
    def _get_downloadable_resources_section(page):
        return page.find('div', class_='pane-downloadable-resources')

    @staticmethod
    def _get_related_resources_section(page):
        return page.find('div', class_='pane-related-items')

    def _scrape_math_lesson(self, parent, lesson, asset_resolver):
        lesson_url = lesson['url']
        lesson_page = self.get_parsed_html_from_url(lesson_url)
        title = lesson['title']
        description = EngageNYChef._get_description(lesson_page)
        translate = self._
        language = self._lang
        lesson_data = dict(
            kind=content_kinds.TOPIC,
            source_id=lesson_url,
            title=translate(title),
            description=translate(description),
            language=language,
            thumbnail=EngageNYChef._get_thumbnail_url(lesson_page),
            children=[],
        )
        resources_pane = EngageNYChef._get_downloadable_resources_section(lesson_page)

        if resources_pane is None:
            return

        resources_table = resources_pane.find('table')
        resources_rows = resources_table.find_all('tr')

        for row in resources_rows:
            doc_link = row.find_all('td')[1].find('a')
            description = EngageNYChef.get_text(doc_link)
            title = EngageNYChef.strip_byte_size(description)
            sanitized_doc_link = doc_link['href'].split('?')[0]
            doc_path = EngageNYChef.make_fully_qualified_url(sanitized_doc_link)
            if 'pdf' in doc_path:
                document_node = dict(
                    kind=content_kinds.DOCUMENT,
                    source_id=os.path.basename(doc_path),
                    title=translate(title),
                    author='Engage NY',
                    description=translate(description),
                    license=EngageNYChef.ENGAGENY_LICENSE,
                    thumbnail=None,
                    files=[dict(
                        file_type=content_kinds.DOCUMENT,
                        path=asset_resolver(doc_path),
                        language=language,
                    )],
                    language=language,
                )
                lesson_data['children'].append(document_node)

        parent.append(lesson_data)

    def _build_scraping_json_tree(self, web_resource_tree):
        channel_tree = dict(
            source_domain='engageny.org',
            source_id='engageny_' + self._lang,
            title=self._(f'EngageNY ({self._lang})'),
            description=self._('EngageNY Common Core Curriculum Content, ELA and CCSSM combined'),
            language=self._lang,
            thumbnail='./content/engageny_logo.png',
            children=[],
        )
        self._scrape_ela_grades(channel_tree, web_resource_tree['children']['ela']['grades'])
        self._scrape_math_grades(channel_tree, web_resource_tree['children']['math']['grades'])
        return channel_tree

    def _scraping_part(self, json_tree_path, options):
        """
        Download all categories, subpages, modules, and resources from engageny and
        store them as a ricecooker json tree in the file `json_tree_path`.
        """
        # Read web_resource_trees.json
        with open(os.path.join(EngageNYChef.TREES_DATA_DIR, EngageNYChef.CRAWLING_STAGE_OUTPUT)) as json_file:
            web_resource_tree = json.load(json_file)
            assert web_resource_tree['kind'] == 'EngageNYWebResourceTree'

        if not self._lang:
            self._setup_language(options)

        # Build a Ricecooker tree from scraping process
        ricecooker_json_tree = self._build_scraping_json_tree(web_resource_tree)
        self._logger.info(f'Finished building {json_tree_path}')

        # Write out ricecooker_json_tree_{lang_code}.json
        write_tree_to_json_tree(json_tree_path, ricecooker_json_tree)

    # endregion Scraping

    def pre_run(self, args, options):
        """
        Run the preliminary parts.
        """
        self._setup_language(options)
        self.crawl(args, options)
        self.scrape(args, options)

    def _setup_language(self, options):
        supported_languages = ', '.join(self.SUPPORTED_LANGUAGES)
        lang = EngageNYChef._get_lang(**options)
        if not lang:
            print(f'\n`lang` is a required argument, choose from one of: {supported_languages}')
            exit(-1)
        if lang not in self.SUPPORTED_LANGUAGES:
            print(f'\n`{lang}` is not a supported language, try one of: {supported_languages}')
            exit(-1)
        self._lang = lang
        self.translation_client = translation.Client(target_language=self._lang)

    @staticmethod
    def _get_lang(**kwargs):
        lang = kwargs.get('lang')
        return lang if lang else None

# endregion Chef

# region Integration testing


def __get_testing_chef():
    http_session = create_http_session(EngageNYChef.HOSTNAME)
    logger = create_logger()
    return EngageNYChef(http_session, logger)

# endregion Integration testing

# region CLI


if __name__ == '__main__':
    chef = EngageNYChef(create_http_session(EngageNYChef.HOSTNAME), create_logger())
    chef.main()

# endregion CLI
