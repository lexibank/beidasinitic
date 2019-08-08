import attr
import lingpy
from clldutils.path import Path
from clldutils.text import strip_chars, split_text_with_context
from clldutils.misc import lazyproperty
from lingpy.sequence.sound_classes import syllabify
from pylexibank.dataset import Concept, Language
from pylexibank.dataset import NonSplittingDataset as BaseDataset
from pylexibank.util import pb, getEvoBibAsBibtex

@attr.s
class BDConcept(Concept):
    Chinese = attr.ib(default=None)

@attr.s
class HLanguage(Language):
    Latitude = attr.ib(default=None)
    Longitude = attr.ib(default=None)
    ChineseName = attr.ib(default=None)
    SubGroup = attr.ib(default='Sinitic')
    Family = attr.ib(default='Sino-Tibetan')
    DialectGroup = attr.ib(default=None)

class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "beidasinitic"
    concept_class = BDConcept
    language_class = HLanguage

    def cmd_download(self, **kw):
        self.raw.write("sources.bib", getEvoBibAsBibtex("Cihui", **kw))

    def cmd_install(self, **kw):
        wl = lingpy.Wordlist(self.raw.posix("words.tsv"), conf=self.raw.posix("wordlist.rc"))

        with self.cldf as ds:
            ds.add_sources(*self.raw.read_bib())
            ds.add_concepts(id_factory=lambda c: c.number)
            langs = {k['Name']: k['ID'] for k in self.languages}
            ds.add_languages()

            for k in pb(wl, desc="wl-to-cldf", total=len(wl)):
                if wl[k, "value"]:
                    ds.add_lexemes(
                        Language_ID=langs[wl[k, "doculect"]],
                        Parameter_ID=wl[k, "beida_id"],
                        Value=wl[k, "value"],
                        Segments=syllabify(self.tokenizer(
                            '', '^'+''.join(wl[k, 'segments'])+'$',
                            column='IPA'), cldf=True),
                        Source="Cihui",
                    )
