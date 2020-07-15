import attr
from pathlib import Path

from pylexibank import Concept, Language, Lexeme
from pylexibank import Dataset as BaseDataset
from pylexibank.util import progressbar, getEvoBibAsBibtex

from clldutils.misc import slug

import lingpy
from lingpy.sequence.sound_classes import syllabify


@attr.s
class CustomConcept(Concept):
    Chinese_Gloss = attr.ib(default=None)
    Number = attr.ib(default=None)


@attr.s
class CustomLanguage(Language):
    ChineseName = attr.ib(default=None)
    SubGroup = attr.ib(default="Sinitic")
    Family = attr.ib(default="Sino-Tibetan")
    DialectGroup = attr.ib(default=None)


@attr.s
class CustomLexeme(Lexeme):
    Benzi = attr.ib(default=None)


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "beidasinitic"
    concept_class = CustomConcept
    language_class = CustomLanguage
    lexeme_class = CustomLexeme

    def cmd_download(self, **kw):
        self.raw_dir.write("sources.bib", getEvoBibAsBibtex("Cihui", **kw))

    def cmd_makecldf(self, args):
        # load data as wordlists, as we need to bring the already segmented
        # entries in line with clts
        wl = lingpy.Wordlist(
            self.raw_dir.joinpath("words.tsv").as_posix(),
            conf=self.raw_dir.joinpath("wordlist.rc").as_posix(),
        )
        wl.add_entries(
            "new_segments",
            "segments",
            lambda x: syllabify(
                self.tokenizer({}, "^" + "".join(x) + "$", column="IPA"), cldf=True
            ),
        )

        args.writer.add_sources()

        # note: no way to easily replace this with the direct call to `add_concepts`
        # as we add the Chinese gloss via concept.attributes
        concept_lookup = {}
        for concept in self.conceptlists[0].concepts.values():
            idx = concept.id.split("-")[-1] + "_" + slug(concept.gloss)
            args.writer.add_concept(
                ID=idx,
                Name=concept.gloss,
                Chinese_Gloss=concept.attributes["chinese"],
                Number=concept.number,
                Concepticon_ID=concept.concepticon_id,
                Concepticon_Gloss=concept.concepticon_gloss,
            )
            concept_lookup[concept.number] = idx

        language_lookup = args.writer.add_languages(lookup_factory="Name")

        for k in progressbar(wl, desc="wl-to-cldf", total=len(wl)):
            if wl[k, "value"]:
                args.writer.add_form_with_segments(
                    Language_ID=language_lookup[wl[k, "doculect"]],
                    Parameter_ID=concept_lookup[wl[k, "beida_id"]],
                    Value=wl[k, "value"],
                    Form=wl[k, "form"],
                    Segments=wl[k, "new_segments"],
                    Source="Cihui",
                    Benzi=wl[k, "benzi"],
                )

        # We explicitly remove the ISO code column since the languages in
        # this datasets do not have an ISO code.
        args.writer.cldf["LanguageTable"].tableSchema.columns = [
            col
            for col in args.writer.cldf["LanguageTable"].tableSchema.columns
            if col.name != "ISO639P3code"
        ]