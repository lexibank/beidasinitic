# coding=utf-8
from __future__ import unicode_literals, print_function
from itertools import groupby

import attr
import lingpy
from pycldf.sources import Source

from clldutils.path import Path
from pylexibank.dataset import Metadata, Concept
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.lingpy_util import getEvoBibAsBibtex
from pylexibank.util import pb


@attr.s
class BDConcept(Concept):
    Chinese_Gloss = attr.ib(default=None)


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    concept_class = BDConcept

    def cmd_download(self, **kw):
        self.raw.write('sources.bib', getEvoBibAsBibtex('Cihui', **kw))

    def cmd_install(self, **kw):
        wl = lingpy.Wordlist(self.raw.posix('words.tsv'))

        with self.cldf as ds:
            ds.add_sources(*self.raw.read_bib())
            for k in pb(wl, desc='wl-to-cldf'):
                if wl[k, 'value']:
                    ds.add_language(
                        ID=wl[k, 'doculect'],
                        Name=wl[k, 'doculect'],
                        Glottocode=wl[k, 'glottolog'])
                    ds.add_concept(
                        ID=wl[k, 'concept'],
                        Name=wl[k, 'concept'],
                        Concepticon_ID=wl[k, 'concepticon_id'],
                        Chinese_Gloss=wl[k, 'chinese'])
                    ds.add_lexemes(
                        Language_ID=wl[k, 'doculect'],
                        Parameter_ID=wl[k, 'concept'],
                        Value=wl[k, 'value'],
                        Form=wl[k, 'form'],
                        Segments=wl[k, 'segments'],
                        Source='Cihui')
