# coding=utf-8
from __future__ import unicode_literals, print_function
from itertools import groupby

import attr
import lingpy
from pycldf.sources import Source

from lingpy.sequence.sound_classes import syllabify

from clldutils.path import Path
from clldutils.misc import slug
from clldutils.misc import lazyproperty
from pylexibank.dataset import Metadata, Concept
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.util import pb, getEvoBibAsBibtex



@attr.s
class BDConcept(Concept):
    Chinese = attr.ib(default=None)


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = 'beidasinitic'
    concept_class = BDConcept

    def cmd_download(self, **kw):
        self.raw.write('sources.bib', getEvoBibAsBibtex('Cihui', **kw))

    def cmd_install(self, **kw):
        wl = lingpy.Wordlist(self.raw.posix('words.tsv'), 
                conf=self.raw.posix('wordlist.rc'))

        with self.cldf as ds:
            ds.add_sources(*self.raw.read_bib())
            ds.add_concepts(id_factory=lambda c: c.number)
            ds.add_languages(id_factory=lambda c: c['ID'])
            for k in pb(wl, desc='wl-to-cldf', total=len(wl)):
                if wl[k, 'value']:
                    ds.add_lexemes(
                        Language_ID=wl[k, 'doculect'],
                        Parameter_ID=wl[k, 'beida_id'],
                        Value=wl[k, 'value'],
                        Form=wl[k, 'form'],
                        Segments=syllabify(
                            [{'t↑h': 'tʰ', 'ᴇ': 'ᴇ/ɛ̝'}.get(
                                x, x) for x in wl[k, 'segments']]
                            ),
                        Source='Cihui')
