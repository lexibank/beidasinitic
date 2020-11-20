from pathlib import Path

import attr
import lingpy
import pylexibank
from pylexibank import Dataset as BaseDataset
from clldutils.misc import slug
from lingpy.sequence.sound_classes import syllabify

from cldfbench import CLDFSpec
from csvw import Datatype
from pyclts import CLTS


@attr.s
class CustomConcept(pylexibank.Concept):
    Chinese_Gloss = attr.ib(default=None)
    Number = attr.ib(default=None)


@attr.s
class CustomLanguage(pylexibank.Language):
    ChineseName = attr.ib(default=None)
    SubGroup = attr.ib(default="Sinitic")
    Family = attr.ib(default="Sino-Tibetan")
    DialectGroup = attr.ib(default=None)


@attr.s
class CustomLexeme(pylexibank.Lexeme):
    Benzi = attr.ib(default=None)


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "beidasinitic"
    concept_class = CustomConcept
    language_class = CustomLanguage
    lexeme_class = CustomLexeme
    form_spec = pylexibank.FormSpec(replacements=[("❷", ""), ("&quot;", "")])

    def cmd_download(self, **kw):
        self.raw_dir.write("sources.bib", pylexibank.getEvoBibAsBibtex("Cihui", **kw))

    def cldf_specs(self):
        return {
            None: BaseDataset.cldf_specs(self),
            'structure': CLDFSpec(
                module='StructureDataset',
                dir=self.cldf_dir,
                data_fnames={'ParameterTable': 'features.csv'}
            )
        }

    def cmd_makecldf(self, args):
        with self.cldf_writer(args) as writer:
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

            writer.add_sources()

            # note: no way to easily replace this with the direct call to `add_concepts`
            # as we add the Chinese gloss via concept.attributes
            concept_lookup = {}
            for concept in self.conceptlists[0].concepts.values():
                idx = concept.id.split("-")[-1] + "_" + slug(concept.gloss)
                writer.add_concept(
                    ID=idx,
                    Name=concept.gloss,
                    Chinese_Gloss=concept.attributes["chinese"],
                    Number=concept.number,
                    Concepticon_ID=concept.concepticon_id,
                    Concepticon_Gloss=concept.concepticon_gloss,
                )
                concept_lookup[concept.number] = idx

            language_lookup = writer.add_languages(lookup_factory="Name")

            for k in pylexibank.progressbar(wl, desc="wl-to-cldf", total=len(wl)):
                if wl[k, "value"]:
                    form = self.form_spec.clean(form=wl[k, "value"], item=None)

                    writer.add_form_with_segments(
                        Language_ID=language_lookup[wl[k, "doculect"]],
                        Parameter_ID=concept_lookup[wl[k, "beida_id"]],
                        Value=wl[k, "value"],
                        Form=form,
                        Segments=wl[k, "new_segments"],
                        Source="Cihui",
                        Benzi=wl[k, "benzi"],
                    )

            # We explicitly remove the ISO code column since the languages in
            # this datasets do not have an ISO code.
            writer.cldf["LanguageTable"].tableSchema.columns = [
                col
                for col in writer.cldf["LanguageTable"].tableSchema.columns
                if col.name != "ISO639P3code"
            ]
            language_table = writer.cldf['LanguageTable']
            
        with self.cldf_writer(args, cldf_spec='structure', clean=False) as writer:
            writer.cldf.add_component(language_table)
            writer.objects['LanguageTable'] = self.languages
            inventories = self.raw_dir.read_csv('inventories.tsv',
                    normalize='NFC', delimiter='\t', dicts=True)
            writer.cldf.add_columns(
                    'ParameterTable',
                    {'name': 'CLTS_BIPA', 'datatype': 'string'},
                    {'name': 'CLTS_Name', 'datatype': 'string'},
                    {
                        'name': 'Lexibank_BIPA',
                        'datatype': 'string',
                    },
                    {'name': 'Prosody', 'datatype': 'string'},
                    )
            writer.cldf.add_columns(
                    'ValueTable',
                    {'name': 'Context', 'datatype': 'string'}
                    )
            clts = CLTS(args.clts.dir)
            bipa = clts.transcriptionsystem_dict['bipa']
            td = clts.transcriptiondata_dict['beidasinitic']
            pids, visited = {}, set()
            for row in pylexibank.progressbar(inventories, desc='inventories'):
                if not row['Value'].startswith('(') and row['Value'] != 'Ø':
                    for s1, s2, p in zip(
                            row['Value'].split(),
                            row['Lexibank'].split(),
                            row['Prosody'].split()):
                        pidx = '-'.join([str(hex(ord(s)))[2:].rjust(4, '0') for s in
                            s1])+p

                        if not s1 in td.grapheme_map:
                            args.log.warn('missing sound {0} / {1}'.format(
                                s1, ' '.join([str(hex(ord(x))) for x in s1])))
                        else:
                            sound = bipa[td.grapheme_map[s1]]
                            sound_name = sound.name if sound.type not in [
                                'unknown', 'marker'] else ''
                            if not pidx in visited:
                                visited.add(pidx)
                                writer.objects['ParameterTable'].append({
                                    'ID': pidx,
                                    'Name': s1,
                                    'Description': sound_name,
                                    'CLTS_BIPA': td.grapheme_map[s1],
                                    'CLTS_Name': sound_name,
                                    'Lexibank_BIPA': s2,
                                    'Prosody': p
                                    })
                            writer.objects['ValueTable'].append({
                                'ID': row['Language_ID']+'_'+pidx,
                                'Language_ID': row['Language_ID'],
                                'Parameter_ID': pidx,
                                'Value': s1,
                                'Context': p,
                                'Source': ['Cihui']
                                })

