# Digital tvilling av produksjonsprosessen med maskin og operatør

| Metadata | Verdi |
| --- | --- |
| Dokumenttype | Produkt- og utviklingsspesifikasjon; forslag til gjennomgang |
| Godkjent av eier | Definisjonen i avsnitt 1 og valget av produksjonsprosessen med maskin og operatør som faktisk motpart |
| Ikke godkjent gjennom definisjonen alene | Detaljerte modellregler, terskler, implementasjon, produksjonsaktivering eller påstander om oppnådd effekt |
| Kontrollgrunnlag | Lokal kildegjennomgang; eksakt målcommit skal bindes i tilgangsstyrt D0-inventar før kodearbeid |
| Konfidensialitet | Produksjonsdata og datakartlegging er konfidensielle; dokumentasjonen skal bruke struktur og plassholdere |
| Første releaseforslag | [Plan med arbeidsnavn v1.0.1](../roadmap/v1_0_1_plan.md); avgrenser første historikkleveranse, ikke hele denne spesifikasjonen |
| Krav og aksept | [Kravspesifikasjon](production_process_digital_twin_requirements.md) med stabile krav-ID-er, gjenbruksmatrise og verifikasjon |
| Samlet bindende oppførsel | [F01–F14 i kravspesifikasjonen](production_process_digital_twin_requirements.md#f01-hva-tvillingen-gjør-fra-start-til-slutt) presiserer hele den menneskemedierte saksgangen, egenskaper, sammenligning og scenario foran eldre domeneforslag nedenfor |
| Konkret atferd | [Atferdskontrakt](production_process_digital_twin_behavior.md) presiserer første leveranses felt, brukerflyt, tilstand, feil og faste grenser foran eldre tekniske forslag |
| Omfang for denne leveransen | Dokumentasjon. Ingen produktkode, datakilder, opptak, modeller eller kjørende tjenester endres |

## 1. Godkjent definisjon

> En digital tvilling er en formålsbestemt digital representasjon av en identifiserbar faktisk motpart — eksempelvis et objekt, system, en tjeneste eller en prosess. Tvillingen oppdateres med observasjoner fra motparten og bruker modeller til å beskrive, analysere, simulere eller forutsi utvalgte egenskaper og oppførsel. Den leverer tjenester som støtter definerte, etterprøvbare mål, og inngår i en tilbakemeldingssløyfe der resultatene kan påvirke motparten og effekten vurderes gjennom nye observasjoner.

Motparten kan være fysisk, digital, organisatorisk eller sammensatt. For denne
tvillingen er den **produksjonsprosessen med maskin og operatør**. Detaljer om
maskinen representeres i den grad de er relevante for prosessen og støttes av data.

DarTwin gir begreper for å knytte formål og mål til tvillinger og forbindelser.
Den godkjente definisjonen er en norsk prosjektdefinisjon basert på dette
arbeidet, ikke et ordrett sitat eller en påstand om full DarTwin-konformitet.

## 2. Produktmål og første nyttige resultat

Tvillingen skal gjøre det mulig å følge ett produksjonsforløp fra registrerte
maskinhendelser, gjennom operatørens observasjon og beslutning, til rapportert
handling og etterfølgende resultat. Den skal vise hvilke deler som er målt,
hvilke som er menneskelige beskrivelser, og hvilke som er modellens slutninger.

Det første nyttige resultatet er en **etterprøvbar prosesshistorie**. En bruker
skal kunne velge en maskin og et tidsrom, undersøke forløp og operasjonsepisoder,
knytte relevant operatørkunnskap til disse og finne grunnlaget for hver påstand.

Deretter skal tvillingen støtte sammenligning med relevante tidligere forløp,
forklarbare avviksvurderinger og registrering av tiltak og resultater. Prediksjon
og årsaksbasert optimalisering er senere utvikling med egne datakrav.

Det samlede målbildet skal kunne svare på følgende. Første avgrensede
historikkleveranse er definert i kravspesifikasjonen og planforslaget:

1. Hva observerte vi i denne delen av produksjonsprosessen?
2. Hvilket program, verktøy og hvilken prosesskontekst er faktisk kjent?
3. Hva observerte, vurderte og besluttet operatøren?
4. Hvilken handling er rapportert utført, og når?
5. Hvilke resultater eller kvalitetsmålinger er registrert etterpå?
6. Finnes det tilstrekkelig sammenlignbare tidligere forløp?
7. Hva vet vi ikke, og hvilke konklusjoner kan vi derfor ikke trekke?

Økt produksjon, bedre kvalitet eller redusert nedetid er ønskede virkninger.
De skal ikke rapporteres som oppnådd før de har egne målinger, sammenligningsgrunnlag
og en dokumentert evalueringsmetode.

## 3. Hva som faktisk finnes i dag

### 3.1 Lokal, direkte datakartlegging

En lesende kartlegging bekrefter at det finnes historisk JSONL-telemetri og
eksisterende analysekode. Konkrete maskinidentiteter, datoer, tidsvinduer,
filinventar, volum, aktivitetsmønstre og dataverdier skal ikke gjengis her.
Detaljert inventar skal bare behandles i det autoriserte, konfidensielle miljøet.

At materialet lar seg lese, verifiserer ikke målenøyaktighet, tidsone,
dubletter, felles maskinidentitet eller produksjonsmessig gyldighet. Et maskinnavn
er ikke i seg selv bevis for at ulike opptak gjelder samme faktiske maskin.

Felt som finnes, inkluderer `execution`, `mode`, `program`, `Tool_number`,
`Srpm`, `Sload`, `Stemp`, aksebelastninger, mating, overstyringer og
`PartCountAct`. Deler av materialet har underprogram, linje, verktøygruppe og
akseposisjoner. Feltforekomst betyr ikke at verdiene er gyldige, har kjente
enheter eller er målt samtidig. Det skal kontrolleres før modellen bruker dem.

Tidsstempeltekstene i dette legacy-materialet mangler synlig tidssone.
Eksisterende analyser behandler tid som UTC; dette er en kompatibilitetsregel
som må skilles fra verifisert kildetid før tidskobling til operatørhendelser.

Tilgjengelig operatør-, handlings- og resultatevidens må kartlegges i det
konfidensielle pilotomfanget. En kodekontrakt eller datamodell dokumenterer ikke
at slike registreringer faktisk finnes. Lokal kartlegging sier heller ikke
hva som finnes i Federation eller på andre installasjoner.

### 3.2 Eksisterende byggesteiner

| Byggestein | Direkte observert i kildekoden | Begrensning |
| --- | --- | --- |
| Recorder og kanoniske observasjoner | `catalog/mtconnect_recorder/canonical/` | Tilgjengelig opptaksarkiv og kjørende datatilførsel er ikke verifisert av denne dokumentleveransen |
| Operasjonell lagdeling | `operational/roles.py`, `timeline.py`, `runs.py`, `cycles.py`, `episodes.py`, `store.py`, `projection.py` | Kode og tester finnes; dette er ikke ny dokumentasjon av merge-, release- eller fysisk akseptstatus |
| Eksisterende DT-side | `digital_twin_routes.py`, `services/digital_twin_service.py` | Lesende historisk tidsregnskap med aritmetisk scenario |
| Operatørnotater | `services/operator_strategy_service.py` | Legacy-feltnotater; ikke bevis for kanonisk OSL-konformitet eller faktisk utførte handlinger |
| Støttekort og bekreftelser | `operator_support_model.py`, `operator_support_service.py`, `operator_confirmation_service.py` | Kort, anbefaling, bekreftelse, handling og effekt må holdes atskilt |
| Operatørkunnskap og SysML | `docs/implementation/operator_knowledge_modeling.md` | Vedlikeholdt referanse for eksisterende notatflyt og teksteksport; ingen påstand om formell språkkonformitet |

Den eldre segmenteringsplanens statustabell sier at S1–S7 ikke er levert,
samtidig som den undersøkte checkouten inneholder store deler av kjeden.
Før kodearbeid må D0 nedenfor avklare dette mot faktiske commits, tester og
akseptbevis. Dokumentet endrer ikke den gamle statustabellen eller erklærer
segmenteringssporet ferdig.

### 3.3 Hva dagens «hva om» betyr

Eksisterende `/digital-twin` beregner observerte aktive/ikke-aktive intervaller
og lar brukeren omfordele en andel av klassifisert ikke-aktiv tid til aktiv tid.
Det er en nyttig, avgrenset tjeneste. Den er ikke en validert modell av hvordan
et konkret operatørtiltak endrer produksjonen. Dagens atferd beholdes under sitt
eksisterende kontraktssett; den skal ikke få sterkere påstander gjennom ny navngiving.

## 4. Systemgrense og representasjon

### 4.1 Den faktiske motparten

En prosessinstans består av relevante sammenhenger mellom:

- maskinen og dens observerte tilstand;
- arbeidsoppgaven og operasjonen, når disse er identifisert;
- program, verktøy, oppspenning og materiale, i den grad opplysningene finnes;
- operatørens registrerte observasjoner, beslutninger og handlinger;
- registrerte resultater, målinger og oppfølging.

Prosessen avgrenses innledningsvis til én identifisert maskin og dens tilknyttede
operatørhendelser per visning og evaluering. Fleremaskinlinjer, materialflyt,
skiftplanlegging og tverrgående produksjonsoptimalisering kommer senere.

Operatørens registrerte handlinger representeres som en del av prosessen.
Tvillingen skal ikke utlede personlighet, kompetansenivå, tilstedeværelse eller
individuell produktivitet fra maskinsignalene.

### 4.2 Tvillingen og FCP

Tvillingen består av en prosessmodell, et datagrunnlag, eksplisitte analysemodeller
og tjenester som bruker disse. FCP er programvaren som lagrer, forbinder og
presenterer dem. Federation er infrastruktur for autorisert tilgang og transport.
At transporten virker, betyr ikke at prosessmodellen er validert.

En visualisering, et språkmodell-svar eller en SysML-fil er en mulig
representasjon eller tjeneste; ingen av disse er alene tvillingen.

## 5. Formål, mål og egenskaper

| Mål | Tjeneste | Egenskaper av interesse | Etterprøvbar leveranse |
| --- | --- | --- | --- |
| G1: Forstå forløpet | Historie og avspilling | Tilstand, kontekst, varighet og datakvalitet | Alle viste intervaller kan spores til inngangsgrunnlag og modellversjon |
| G2: Forbinde erfaring og måling | Hendelses- og evidenskobling | Observasjon, beslutning, handling og resultat | En hendelseskjede med eksplisitte eller uavklarte koblinger |
| G3: Sammenligne relevante forløp | Historisk sammenligning | Operasjonskontekst, varighet og måleegenskaper | Inkluderte og utelatte referanseepisoder med begrunnelse |
| G4: Oppdage forskjeller | Avviksvurdering | Forskjell fra en godkjent referanse og dens gyldighetsområde | En forklarbar vurdering eller en eksplisitt avståelse |
| G5: Følge opp tiltak | Tilbakemeldingssløyfe | Utført tiltak, etterfølgende observasjon og målt resultat | Registrert resultat uten automatisk årsakspåstand |
| G6: Undersøke antakelser | Avgrensede scenarioer | Valgte forutsetninger og beregnede konsekvenser | Reproduserbar beregning med synlige antakelser |

G1–G2 er første produktmilepæl. G3–G6 skal ha separate leveranser og
gyldighetskrav. Tilbakemeldingssløyfen er i første omgang menneskemediert:
operatøren velger og utfører eventuelle tiltak, og nye observasjoner registreres.
Dette innebærer ingen påstand om automatisk toveis maskinstyring.

### 5.1 Rolleprofil

Definisjonsrevisjonen skal angi rollefordeling, inspirert av forskningen
oppsummert i [forskningsgrunnlaget](production_process_digital_twin_research.md).

| Rolle | Første prosesstvilling | Menneskelig bidrag |
| --- | --- | --- |
| Observatør | Les autoriserte målinger og registrerte hendelser | Registrer observasjoner og korriger kontekst med egen kildeklasse |
| Analytiker | Rekonstruer forløp og utfør bare erklærte, validerte beregninger | Vurder betydning, sammenlignbarhet og manglende opplysninger |
| Beslutningstaker | Registrer beslutning og dens grunnlag | Velg eller avstå fra tiltak |
| Utfører | Registrer rapportert utførelse; ingen maskinkommando | Utfør eventuelt tiltak gjennom ordinær prosess |

En rolle kan få ny automatiseringsgrad bare gjennom en ny gjennomgått
definisjons-/tjenesterevisjon. Analyse av data gir ikke automatisk beslutnings-
eller utførelsesrett.

## 6. Foreslått arkitektur

```text
Faktisk produksjonsprosess
    maskinobservasjoner                 operatør / kvalitetsmåling
             |                                      |
    eksisterende Recorder                 opprinnelig notat / ny hendelse
             |                                      |
    verifiserte rådata                    versjonert kildehenvisning
             |                                      |
    kanoniske observasjoner                         |
             |                                      |
    eksisterende runs / cycles / episodes ----------+
                         |
              versjonert prosessnapshot
                         |
          historie / koblinger / egenskaper
                         |
           sammenligning / avvik / scenario
                         |
              menneskelig beslutning
                         |
          rapportert handling i faktisk prosess
                         |
             nye observasjoner / resultat
                         +----> ny snapshot og oppfølging
```

Ny domene- og applikasjonslogikk foreslås i `catalog/digital_twin/`.
Recorderens kanoniske observasjoner og segmentering konsumeres gjennom
leseadaptere. HTTP-ruter skal ikke inneholde egne segmenteringsregler.

Legacy JSONL får en egen adapter. Den får ikke konstruere falske MTConnect-
instanser eller sekvensbevis for å passe inn i den sterkere rådatakontrakten.

## 7. Felles datakontrakt og ufravikelige regler

### 7.1 Identitet og kildeomfang

Alle evidensreferanser skal bære `evidence_dataset_id`, som identifiserer et
registrert kildedatasett og dets opptaksopprinnelse. Datasettregistreringen skal
inkludere opprinnelsesinstallasjon hvis kjent, kildeinventar og innholdsdigester.
Ukjent opprinnelse skal være uttrykkelig, ikke erstattes med lokal installasjonsidentitet.

Innen et kanonisk datasett beholdes naturlig observasjonsidentitet
`(source_key, agent_instance_id, sequence)` og operasjonelt enhetsomfang
`(source_key, agent_instance_id, device_key)`. Datasettomfanget hindrer kollisjon
når ulike installasjoner bruker like kildenavn og Agent-instansverdier.

Kopier av samme opptak skal kunne identifiseres gjennom innhold og opprinnelse.
De skal ikke telle dobbelt som uavhengige trenings- eller evalueringsforløp.
Hvis overlapping ikke kan avgjøres, sperres sammenslåing til én referansegruppe.

Legacy-rader identifiseres med datasett, filens SHA-256 og fysisk linjenummer.
Et felt kalt `sequence` skal ikke tolkes som kanonisk observasjonssekvens uten
kontroll av formatets faktiske semantikk. Maskinnavn kobles til en faktisk
maskin gjennom en versjonert og eksplisitt identitetskobling.

### 7.2 Tid, kontinuitet og målekvalitet

- Kanonisk sekvens er rekkefølgeautoritet innen en Agent-instans.
- Sekvenshull vurderes på hele Agent-strømmen før filtrering per enhet.
- Ingen inferert episode eller arvet kontekst skal dekke et reelt evidenshull.
- Tidsregresjon skal gi en navngitt diskontinuitet og aldri negativ varighet.
- Alle nye tidsvinduer er halvåpne `[start_at, end_at)` i UTC.
- Original tidsstempeltekst og kilde til tidsnormalisering beholdes.
- Legacy-tid uten sone kan vises med eksplisitt antakelse, men kan ikke
  automatisk kobles til operatørhendelser før kildeklokken er avklart.
- `observed_at`/`occurred_at` og `recorded_at` er ulike felt. Et notat skrevet
  senere er ikke en samtidighetsmåling.
- `UNAVAILABLE`, manglende, tvetydig og utenfor gyldighetsområdet beholdes som
  ulike tilstander. Ingen av dem blir automatisk null, stopp eller normaldrift.
- Lange intervaller mellom endringsbaserte MTConnect-observasjoner er ikke
  alene bevis for datatap. Legacy-sidens 30-sekundersregel skal ikke overføres
  til den kanoniske segmenteringen.

### 7.3 Bevis, beskrivelser og slutninger

Hver påstand skal merkes med én av `OBSERVED`, `HUMAN_REPORTED`,
`DERIVED`, `INFERRED` eller `HYPOTHETICAL`, samt kildereferanser og versjon.
Ett resultat kan inneholde flere påstander med ulike klasser.

`ACTIVE` betyr ikke at en godkjent del ble produsert. En
`PRODUCTION_CANDIDATE` skal beholde denne klassifikasjonen. En endring i
`PartCountAct` er en tellerhendelse inntil tellerens betydning, resetter og
tilknytning til produkt og kvalitetsutfall er avklart.

Et foreslått tiltak er ikke en beslutning. En beslutning er ikke en utført
handling. En operatørbekreftelse er ikke et målt resultat. Et forbedret resultat
etter et tiltak er ikke alene bevis for at tiltaket forårsaket forbedringen.

### 7.4 Konfidensialitet gjelder også metadata og avledede resultater

Produksjonsdata er konfidensielle som standard. Dette omfatter også datoer,
klokkeslett, maskin-/personidentiteter, program- og verktøyidentiteter,
filnavn, opptaksperioder, radantall, aktivitetsmønstre, notater, resultatmålinger,
skjermbilder og modellresultater som kan avsløre produksjonsforhold.

- Dokumentasjon, kodeeksempler, tester, commits, issues, PR-er og delte
  demonstrasjoner skal bruke gjennomgåtte syntetiske data eller plassholdere.
- Verken kildedata eller en detaljert dataprofil skal kopieres til repoet,
  verktøykall mot eksterne tjenester, ekstern AI eller delbare visualiseringer.
- Reelle tidspunkter og full proveniens beholdes bare i autorisert datalagring.
  Utviklerspesifikasjonen kan beskrive tidsfeltenes format uten å fylle inn data.
- Maskinnavn eller tidspunkt skal ikke havne i URL-parametere, allmenne
  driftslogger, feilmeldinger eller målemetrikker. Detaljert audit er tilgangsstyrt.
- Skjermbilder og eksport er egne utleveringer. Ingen eksport-/delingsfunksjon
  skal aktiveres gjennom denne planen uten definert mottaker, omfang og gjennomgang.
- Bytte av navn med alias eller hashing er ikke tilstrekkelig anonymisering.
  Datoer, mønstre og kombinasjoner av metadata kan fortsatt identifisere kilden.
- Ingen avledet baseline, rapport eller scenario blir mindre konfidensielt
  automatisk fordi det er beregnet fremfor direkte målt.

Datatilgang skal kontrolleres på tvilling, datasett, menneskehendelse og
evidensreferanse. En bruker som kan se et maskinforløp, får ikke dermed tilgang
til alle operatørnotater eller identitetsopplysninger i det samme tidsrommet.
Sikkerhetskopier og eventuelle private inventarer følger samme tilgangs- og
bevaringskrav som primærdataene.

## 8. Foreslåtte domenetyper

Alle nye typer serialiseres med et eksplisitt `schema` og et versjonsnummer.
Tid lagres som UTC med heltallsmikrosekunder der sammenligning og varighet
krever det. Tall som NaN og uendelig er ugyldige i kontraktene.

| Type | Obligatorisk innhold | Semantikk |
| --- | --- | --- |
| `TwinDefinitionRevision` | `twin_id`, `revision_id`, faktisk motpart, maskinbinding, prosessgrense, mål, valgte egenskaper, rolleprofil, tillatte tjenester, gyldighetsområde, forrige revisjon | Uforanderlig definisjon; endret formål, rollefordeling eller omfang krever ny revisjon |
| `DatasetRevision` | `dataset_id`, revisjon, opprinnelse, filinventar og digester, format, kildeklokkepolicy, identitetskoblingsrevisjon, tilgjengelige egenskaper | Eksakt inngangsgrunnlag; nye filer eller endret innhold gir ny revisjon |
| `EvidenceRef` | datasett/revisjon, format, kildeobjekt/digest, locator, evidensklasse | Kanonisk locator har kilde/instans/sekvens/enhet; legacy-locator har filhash/linje; notat har post-ID og innholdshash |
| `TwinSnapshot` | `snapshot_id`, definisjonsrevisjon, datasett-/segmenteringsrevisjoner, tidsvindu, grense for inkluderte observasjoner, opptakstid, tilstand og begrunnelser | Frosset grunnlag for én visning eller analyse; ingen blanding av revisjoner underveis |
| `ProcessContextRevision` | identitet, maskin, operasjon, programrevisjon, verktøykontekst, oppspenning, materiale når nødvendig, forfatter og kildegrunnlag | Hvert felt har verdi eller eksplisitt ukjentstatus; manuelt lagt til kontekst merkes `HUMAN_REPORTED` |
| `EpisodeFeatureSet` | episode-/snapshot-ID, egenskapspolicy, kontekstrevisjon, navngitte mål med enhet, antall observasjoner, kildegrunnlag og kvalitetsstatus | Avledede egenskaper, aldri ny segmentidentitet |
| `EvidenceLinkRevision` | lenke-ID, begge objektreferanser med revisjoner, relasjon, koblingsmetode, status og begrunnelse | Eksplisitt, foreslått eller avvist kobling mellom hendelse og prosessforløp |
| `BaselineRevision` | gruppe, egenskaper/enheter, policy, treningsperiode, eksakte episodefingeravtrykk, fordelinger og gyldighetsområde | Uforanderlig empirisk referanse; endret utvalg gir ny revisjon |
| `DeviationAssessment` | mål-episode, snapshot, baseline, egenskap, verdi, referanse, vurdering, begrunnelser og evidens | Forskjell eller avståelse; ikke automatisk årsak eller alarm |
| `DecisionRecord` | ID, aktør fra autentisert sesjon, hendelsestid, registreringstid, valgt handlingstekst, kontekst/evidens, eventuell støtte-/strategireferanse | Registrerer en menneskelig beslutning; gir ingen maskin- eller strategi-godkjenningsrett |
| `ActionReport` | ID, beslutningsreferanse hvis kjent, hendelsestid, registreringstid, rapporterende aktør, handlingstype og beskrivelse, utførelsesstatus/evidens | Utførelse er `REPORTED_PERFORMED`, `REPORTED_NOT_PERFORMED` eller `UNKNOWN` med kildeklasse |
| `OutcomeObservation` | ID, handling/episodekobling, observasjonstid, kilde, metrikk/enhet eller rapportert vurdering, målemetode og evidens | Skiller målt resultat fra menneskelig vurdering og manglende resultat |
| `FeedbackEvaluation` | handling, før-/etter-snapshots, kontekstforskjeller, utfallsreferanser, evalueringspolicy, konklusjon og begrunnelse | Kan konkludere med beskrevet forskjell, blandet resultat eller utilstrekkelig grunnlag |
| `ScenarioEvaluation` | snapshot, scenariotype, modell/policy, parametere, enheter, forutsetninger, resultater og begrensninger | Hypotetisk; endrer ikke historie eller maskin |
| `ModelValidityAssessment` | modellrevisjon, formål, vurderingsomfang, metode, metrikker, toleransepolicy, evidens og status | Skiller faglig modellgyldighet fra teknisk tilgjengelighet; status er `VALID_WITHIN_SCOPE`, `PARTIAL`, `INVALID` eller `NOT_ASSESSED` |

Primærregistreringer får servergenererte stabile identifikatorer ved opprettelse.
Revisjons-ID/fingeravtrykk beregnes fra kanonisk innhold. Avledede objekters
identitet beregnes deterministisk fra eksakte inngangsrevisjoner og policy.
Innholdshash erstatter ikke menneskelig forfatterskap eller autorisasjon.

### 8.1 Hendelser, prosessobjekter og proveniens

Relasjonsmodellen skal støtte mange-til-mange-koblinger mellom beslutninger,
handlinger, resultater, episoder og prosesskontekst. Hver relasjon bærer
`relation_kind`, kvalifiserende rolle, kilde-/målrevisjon, koblingsstatus og
grunnlag. Foreslåtte koblinger blir ikke bekreftede fordi de kan lagres.

`episode_id` i et enkelt skjema er en forenklet måte å opprette én relasjon
på. Det skal ikke gjøre det umulig å registrere en handling som gjelder flere
episoder. Relasjonene kan senere kartlegges til en valgt OCEL-versjon; første
leveranse implementerer ingen import-/eksportpåstand.

Proveniens skal skille kilde-/resultatobjekt, bearbeidingsaktivitet og ansvarlig
menneske/programvareaktør. Disse kan kartlegges til PROV Entity, Activity og
Agent uten å kreve RDF eller en grafdatabase. Proveniensrelasjonene er verken
årsaksbevis eller autorisasjonsgrants.

## 9. Snapshot, tilstand og tilgjengelighet

Brukeren velger tvilling, maskin og tidsvindu. Tjenesten skal først lese et
konsistent datagrunnlag og etablere en snapshot. Resultater skal referere til
denne snapshoten, også når nye data ankommer mens brukeren undersøker forløpet.

En snapshot har `captured_at`, `last_observed_at`, `data_mode` og kvalitetsstatus.
`data_mode` er `HISTORICAL`, `REPLAY` eller `LIVE`. Live krever dokumentert
oppdateringskilde og en valgt ferskhetspolicy. Avspilling av eldre data skal
aldri se ut som nye maskinobservasjoner.

Ferskhet vurderes ved en eksplisitt `evaluated_at` og inngår ikke i det
deterministiske historiske innholdet. Samme snapshot kan dermed være fersk ved
én vurdering og gammel ved en senere uten at historien omskrives.

Tilgjengelighet beregnes per tjeneste:

| Status | Betydning |
| --- | --- |
| `AVAILABLE` | Datakravene for denne tjenesten og dette omfanget er oppfylt |
| `PARTIAL` | Noe kan vises, med avgrensninger og eksplisitt ukjentgrunnlag |
| `INSUFFICIENT_DATA` | Ingen meningsfull beregning for den forespurte tjenesten |
| `UNRESOLVED_CONTEXT` | Identitet, tid eller sammenligningskontekst er utilstrekkelig |
| `OUTSIDE_VALIDITY` | Data eller forespørsel ligger utenfor modellens erklærte område |
| `UNAVAILABLE` | Kilden eller nødvendig lagring kan ikke leses |

Maskinhistorie kan være tilgjengelig selv om tilbakemelding eller baseline
mangler. En samlet grønn «DT klar»-indikator skal ikke skjule dette.

En tjeneste kan dessuten være teknisk tilgjengelig mens den faglige modellen
er `NOT_ASSESSED` eller `INVALID`. Vurderinger som krever gyldig modell skal
da avstå og vise årsaken. Endret prosess, manglende kontekst, dataproblem og
modellavvik skal holdes atskilt; modellen skal ikke oppdateres automatisk for
å få observasjonene til å passe en ønsket klassifikasjon.

## 10. Rekonstruksjon og hendelseskobling

Kanonisk historie skal bruke eksisterende `MachineRun`, `ProductionCycle` og
`OperationalEpisode`. Grenseregler og begrunnelser videreføres uendret.
`OperationalSegmentationStore` sine spørringer og `CanonicalObservationStore`
brukes gjennom adaptere. Konstruktører som oppretter eller migrerer databaser
må ikke brukes som en skjult skriveoperasjon i en lesende HTTP-forespørsel.

For legacy-materiale skal første leveranse vise eksisterende historiske
oppsummeringer og kildekvalitet. Den skal ikke hevde kanonisk episodeproveniens.
Hvis et nytt legacy-episodeformat er nødvendig, må en separat adapterkontrakt
gjennomgås før implementasjon; manglende råmetadata skal ikke fylles inn som fakta.

Notater og beslutninger kobles i følgende rekkefølge:

1. En eksplisitt brukerangitt episode eller hendelsesreferanse, kontrollert mot
   tvilling, datasett, maskin, revisjon og brukerens tilgang.
2. En foreslått tidskobling når maskinidentitet og kildeklokker er avklart.
3. `UNLINKED` når koblingen ikke kan begrunnes.

Automatisk forslag gir `PROPOSED`, ikke `CONFIRMED`. Flere mulige episoder
skal vises som tvetydighet. Samme tidsstempel eller programnavn er utilstrekkelig
til å bekrefte kobling. Et notat kan gjelde flere episoder; alle relasjoner
registreres eksplisitt. Overlapp innebærer ikke at notatet forklarer hendelsen.

Et endret legacy-notat får nytt innholdsfingeravtrykk. Eksisterende referanser
beholder den tidligere, tilgangsstyrte kilderevisjonen. Manglende original gir
`SOURCE_UNAVAILABLE`; systemet skal ikke bytte til dagens tekst og late som den
var grunnlaget for en tidligere vurdering.

## 11. Egenskaper og historisk sammenligning

### 11.1 Første egenskapssett

Varigheter hentes fra den eksisterende operasjonelle lagdelingen:
veggtid, aktiv tid, programstopp, matehold, annen tilstand og ukjent tid.
Varighetsbøttene skal avstemmes mot totalen med heltallsaritmetikk.

For et numerisk signal kan første egenskapspolicy beregne `sample_count`,
`sample_median`, `sample_p95`, minimum og maksimum innen episodevinduet.
Bruk bare gyldige numeriske verdier med avklart målebetydning og enhet.
`sample_p95` er nærmeste-rang-kvantilen med rang `ceil(0.95 * n)` i sortert utvalg;
medianen er midtverdi eller gjennomsnitt av de to midtverdiene.

Dette er statistikk over rapporterte observasjoner, ikke en tidsvektet
fordeling. Den skal merkes som dette fordi MTConnect kan være endringsbasert.
Ingen interpolasjon, resampling eller videreføring over evidensbrudd inngår i
første policy. Målestrømmer fra ulike komponenter slås ikke sammen bare fordi
de har like enheter eller lignende navn.

En egenskap rapporterer egen støtte: antall målinger, utelatte verdier,
proveniens, enhet og ukjente forhold. Antall punkter erstatter ikke en vurdering
av representativitet eller måledekning.

### 11.2 Hva «sammenlignbar» betyr

En sammenligningsgruppe er et analytisk utvalg, ikke en episodeidentitet.
Ingen gruppering skal endre de eksisterende episode-ID-ene.

Første kvalifiserte varighetssammenligning krever samme faktiske maskin,
operasjonsdefinisjon, programrevisjon, verktøykontekst og oppspenning.
Belastningssammenligning krever i tillegg relevant materialkontekst,
signal/komponent og enhet. Avvik fra kravene skal enten avvise gruppen eller
opprette et separat, eksplisitt utforskende utvalg.

Bare programnummer og verktøynummer er ikke nok: programinnhold, verktøytype,
materiale og oppsett kan ha endret seg. Når kontekst ikke finnes, skal brukeren
kunne undersøke rå historiske forskjeller, men systemet skal avstå fra en
kvalifisert «forventet/avvikende»-klassifikasjon.

Kontekst må være gyldig gjennom den relevante episoden. En startverdi alene
beviser ikke stabilitet. Uavklart program- eller verktøyskifte inne i episoden
utelukker den fra en gruppe som krever stabil kontekst.

### 11.3 Første baseline og dens gyldighet

Foreslåtte tekniske startgrenser er minst 20 kvalifiserte episoder fordelt på
minst tre ulike `MachineRun` og to uavhengige opptaksøkter. Delvise grenser,
ukjent varighet og uavklart nødvendighetskontekst utelukkes fra kvalifisert
varighetsbaseline. Andre egenskaper får egne eksplisitte krav.

Grensene er et forslag til utviklingskontrakt som skal gjennomgås i B0 nedenfor.
De er ikke et bevis for statistisk troverdighet eller et universelt industrikrav.
Stor mengde korrelerte episoder fra samme forløp erstatter ikke uavhengig dekning.

Baseline lagrer utvalgets identitet, median, p05/p95, minimum/maksimum og
gyldighetsområde per egenskap. Ingen førsteversjon skal lære på tvers av maskiner
uten en egen godkjent overføringsmodell. Et nytt utvalg eller endret policy
oppretter en ny baseline-revisjon; den gamle beholdes.

En episode som vurderes skal ikke inngå i egen baseline. Historisk evaluering
skal bruke en kronologisk deling gruppert per opptaksøkt/forløp, slik at framtidige
eller nært sammenfallende treningsdata ikke lekker inn i testgrunnlaget.

### 11.4 Avvik og usikkerhet

En gyldig sammenligning viser målverdi, referansemedian, absolutt forskjell,
prosentvis forskjell når nevneren er ulik null, og empirisk referanseintervall.
Null i nevner gir manglende prosentverdi med begrunnelse.

Utenfor empirisk p05/p95 betyr «utenfor observert referanseintervall».
Det betyr ikke feil, slitasje, farlig tilstand eller statistisk sannsynlighet.
En operativ avviksterskel krever senere en faglig gjennomgått toleransepolicy
per egenskap og kontekst. Manglende toleranse gir `COMPARISON_ONLY`.

Vurderingen skal avstå ved uforenlige enheter, utilstrekkelig kontekst,
ukvalifisert baseline eller mål utenfor modellens gyldighet. En tallfestet
«confidence» skal bare vises hvis den har en definert og evaluert betydning.

## 12. Operatørbeslutning, handling og resultat

Arbeidsflyten skal bevare følgende separate objekter:

```text
observasjon / notat
    -> eventuell forklaring eller støtte
    -> registrert menneskelig beslutning
    -> rapportert handling / ikke utført / ukjent
    -> målt eller rapportert resultat
    -> oppfølging av sammenlignbarhet og effekt
```

I D4 skal en beslutning eller handling kunne registreres retrospektivt.
Derfor er ikke alle tidligere lenker obligatoriske: en handling uten registrert
beslutning lagres med `decision_ref = null` og begrunnelse. Systemet skal ikke
lage en beslutning som brukeren aldri registrerte.

For et nytt tiltak velger brukeren episode/tidsvindu, beskriver beslutning og
eventuell handling, angir faktisk hendelsestid og registrerer resultat senere.
Innlogget aktør bestemmes på serveren. Rapporterende person og personen som
ifølge rapporten utførte handlingen er ulike felt; den sistnevnte kan være ukjent.

Godta/avvis av et støttekort skal aldri automatisk bli `REPORTED_PERFORMED`.
Maskinmålinger skal ikke brukes til å gjette at en bestemt person utførte en
handling. Gjentatt innsending skal ikke skape doble hendelser.

Resultater kan være:

- `MEASURED`: verdi, enhet, målemetode, måletid og kilde er oppgitt;
- `HUMAN_REPORTED`: brukerens vurdering og begrunnelse;
- `NOT_MEASURED`: resultatet mangler;
- `INCONCLUSIVE`: motstridende eller utilstrekkelig grunnlag.

Kvalitetsmålinger krever egenskap, nominell verdi/toleranse når relevant,
måleenhet, målemetode og produkt-/operasjonskobling. Fri tekst om at «det ble
bedre» skal ikke gjøres om til en målt kvalitetsforbedring.

Oppfølging viser før-/etter-vindu, samtidige kontekstendringer og datamangler.
Første evaluering er beskrivende. Årsakseffekt krever et separat forsøk eller
en eksplisitt evaluert kausal metode, og ligger utenfor førsteversjonen.

## 13. Scenarioer og senere beslutningsstøtte

Første støttede scenariotype er eksisterende `TIME_REALLOCATION`:

```text
recovered_time = classified_non_active_time * recovery_fraction
scenario_active_time = observed_active_time + recovered_time
scenario_active_share = scenario_active_time / classified_observed_time
```

Bevar eksisterende grenser 0–50 prosent i trinn på fem prosentpoeng,
minimumskravet på 30 klassifiserte intervaller og den eksisterende klassifiserings-
og intervallpolicyen. Resultatet forblir en hypotetisk tidsomfordeling.

Et scenario må binde inngangs-snapshot, policy og parametere. Det skal ikke
tolkes som at et bestemt tiltak kan hente inn den valgte tiden, øke antall
godkjente deler eller forbedre kvaliteten. Nye scenariomodeller skal få egne
kontrakter og evalueringsgrunnlag.

Senere anbefalinger skal knytte observasjon, egnet kontekst, gyldig kunnskapskilde,
begrunnelse og forventet resultat. Manglende relevant strategi gir ingen anbefaling.
Et språkmodellforslag blir en merket kandidat; det blir ikke etablert årsak,
godkjent strategi eller verifisert maskinhandling.

## 14. OSL, SysML og DarTwin

Eksisterende operatørnotater konsumeres som legacy-kilder med originaltekst,
post-ID, innholdsfingeravtrykk og eventuell oppgitt status. `reusable_strategy`
er ikke bevis for godkjent kanonisk OSL-innhold.

Denne planen oppretter ikke et parallelt språk eller en bakvei for review,
godkjenning, publisering eller SysML-eksport. Eksisterende funksjoner følger
sine eierkontrakter og den vedlikeholdte referansen for operatørkunnskap.
Nye språkfunksjoner krever separat spesifikasjon og bestilling. Prosessens
beslutnings-, handlings- og utfallsregistreringer beskriver hendelser; de
godkjenner ikke en strategi eller dens faglige gyldighet.

DarTwin brukes først til dokumentert sammenheng mellom faktisk motpart, mål,
tvillingmoduler og forbindelser. Automatisk DarTwin/SysML-generering er en
senere adapterleveranse. Ingen ny språkverktøykjede eller avhengighet kreves
for første prosesshistorie.

## 15. API-kontrakt og applikasjonsporter

Dette er foreslåtte nye ruter, ikke eksisterende API. Bruk prefikset
`/api/process-twins/v1` og en egen blueprint. Dagens `/digital-twin` beholdes.

| Metode og relativ rute | Formål | Minimumsrettighet |
| --- | --- | --- |
| `GET /twins` | Les tilgjengelige definisjoner og tjenestestatus | `data.read` |
| `GET /twins/{id}/revisions/{revision}` | Les eksakt definisjon | `data.read` |
| `POST /twins/{id}/snapshots` | Opprett avgrenset snapshot av allerede autoriserte data | `analysis.run` |
| `GET /snapshots/{id}` | Les frosset omfang og kvalitet | `data.read` |
| `GET /snapshots/{id}/timeline` | Sideinndelt prosesshistorie | `data.read` |
| `GET /snapshots/{id}/episodes/{episode_id}` | Episode, kontekst og evidens | `data.read` |
| `GET /snapshots/{id}/links` | Les bekreftede, foreslåtte og uavklarte koblinger | `data.read` |
| `POST /twins/{id}/context-revisions` | Registrer menneskelig prosesskontekst | `workflow.run` |
| `POST /snapshots/{id}/link-revisions` | Opprett eller korriger en evidenskobling | `workflow.run` |
| `POST /twins/{id}/decisions` | Registrer menneskelig beslutning | `workflow.run` |
| `POST /twins/{id}/action-reports` | Registrer utført/ikke utført/ukjent handling | `workflow.run` |
| `POST /twins/{id}/outcomes` | Registrer resultat med kildeklasse | `workflow.run` |
| `POST /twins/{id}/records/{record_id}/corrections` | Legg til lenket korreksjon | `workflow.run` |
| `POST /snapshots/{id}/comparisons` | Beregn sammenligning mot en bestemt baseline-revisjon | `analysis.run` |
| `POST /snapshots/{id}/scenarios` | Beregn et tillatt scenario | `analysis.run` |
| `GET /evaluations/{id}` | Les eksakt sammenligning, scenario eller oppfølging | `data.read` |

For den første historikkleveransen gjelder atferdskontraktens B03–B05/B10:
klargjøring og definisjonsregistrering har eksplisitte beskyttede metadata-
endepunkter og fast rettighetspolicy. Dette presiserer det tidligere
forslaget om applikasjonskommandoer uten angitt inngang. Baseline og nye
prosesshendelser er fortsatt utsatt. Ingen registrering eller analyse
starter ved å åpne en side.

Alle ruter skal bruke sentral menneskelig autorisasjon og kildeomfangskontroll.
`data.read` er nødvendig for kildene også når brukeren har en skriverettighet.
Rettighetsnavnene ovenfor eksisterer i FCP; nye API-endepunkter må registreres
eksplisitt i sentral policy. Ingen endring av standardroller inngår automatisk.

Skriveoperasjoner krever eksisterende CSRF-beskyttelse, gyldig innholdstype,
størrelsesgrenser og serverbundet aktør. Request-body kan ikke overstyre
`recorded_by`, autorisasjon, kildetilgang eller revisjonens innholdsfingeravtrykk.

Korreksjoner og andre revisjonsavhengige kommandoer bærer forventet revisjon.
`Idempotency-Key` kreves på alle POST-ruter: samme nøkkel, aktør og identiske
kanoniske parametere returnerer samme resultat; annet innhold gir konflikt.
Validering, post og audit skal være én transaksjon. Ingen delvis opprettet kjede.

Lesende ruter skal ikke endre kildefiler, starte opptak, opprette databaser,
migrere schema, trene modeller eller starte eksterne jobber.

Foreslåtte svarregler:

- `400`: ugyldig syntaks, parametere eller tidsvindu;
- `401`/`403`: manglende menneskelig identitet eller tilgang;
- `404`: objekt finnes ikke innen brukerens autoriserte omfang;
- `409`: feil revisjon eller gjenbrukt idempotensnøkkel med annet innhold;
- `413`: deklarert størrelsesgrense overskredet;
- `422`: referanser på tvers av maskiner eller semantisk ugyldig kommando;
- `503`: nødvendig kilde-/lagringstjeneste utilgjengelig.

Utilstrekkelig datagrunnlag i en ellers gyldig analyseforespørsel returneres
som et vurderingsobjekt med `INSUFFICIENT_DATA` og begrunnelse, ikke som et
oppdiktet nullresultat eller et internt serverkrasj.

Alle lister har `limit` (standard 100, maksimum 500), stabil sortering og
en ugjennomsiktig cursor bundet til snapshot og filtre. Avkuttede lister skal
vise `has_more`; totalsummer skal ikke beregnes fra en tilfeldig første side.

### 15.1 Konkret request-form

Eksemplene nedenfor er kontraktmaler med plassholdere. De inneholder ingen
produksjonsdatoer, reelle referanser eller verdier fra datakartleggingen.
Tidsplassholderne må erstattes med validerte verdier inne i det autoriserte
miljøet før en faktisk request kan sendes.

En historisk snapshot bestilles med `POST /twins/{id}/snapshots`:

```json
{
  "definition_revision_id": "definition-rev-example",
  "dataset_revision_ids": ["dataset-rev-example"],
  "device_binding_id": "device-binding-example",
  "window": {
    "start_at": "<UTC_START>",
    "end_at": "<UTC_END>"
  },
  "data_mode": "HISTORICAL"
}
```

Serveren kontrollerer at definisjon, datasett og enhetsbinding tilhører samme
autoriserte omfang. Valgt `data_mode` er en forespørsel: klienten kan ikke gjøre
et historisk datasett live ved å endre dette feltet. Bygg bruker et konsistent
inventar og kontrollerer innholdsdigester; endring under lesing gir et nytt
eksplisitt grunnlag eller konflikt, aldri en blandet snapshot.

En beslutning registreres med `POST /twins/{id}/decisions`:

```json
{
  "snapshot_id": "snapshot-example",
  "episode_id": "episode-example",
  "occurred_at": "<UTC_OCCURRED_AT>",
  "decision_text": "Kontrollere arbeidsstykket før videre justering.",
  "evidence_refs": ["evidence-ref-example"]
}
```

`snapshot_id`, `occurred_at` og `decision_text` kreves. `episode_id` kan være
null når episodekobling er uavklart; `evidence_refs` kan være en tom liste,
men resultatet skal da vise at beslutningen mangler koblet evidens.
`decision_text` er 1–65 536 UTF-8-byte. Den er en registrering, ikke en kjørbar kommando.
Serveren setter identifikator, schema, revisjon, `recorded_at` og autentisert
`recorded_by`, og oppretter ikke en handling som følge av denne requesten.

Alle vellykkede opprettelser returnerer `201`, en `Location` til objektet og
revisjonsidentifikator. Idempotent gjentakelse returnerer samme objekt og
samme opprinnelige resultat. Et beregningsobjekt med utilstrekkelig grunnlag
er også et reelt lagret resultat med begrunnelseskoder.

## 16. Brukerflyt og presentasjon

Første brukerflate foreslås som «Prosesstvilling» i Monitor, med lenker til
eksisterende operatørkunnskap i Knowledge. Den skal vise:

1. valgt prosess, maskin, historisk/live/avspillingsmodus og tidsrom;
2. observasjonenes alder og hvilken tjeneste som har manglende grunnlag;
3. en tidslinje for maskintilstand, operatørhendelser og resultater;
4. valgt episode med kontekst, datamangler og åpnbare evidensreferanser;
5. eventuelle sammenlignbare forløp med inkludering/utelatelse forklart;
6. separate skjemaer for beslutning, rapportert handling og resultat.

Brukeren skal kunne lagre et ufullstendig forløp uten å oppgi et fiktivt
resultat. Ukjent og ikke målt skal være eksplisitte valg. Å registrere beslutning
skal ikke forhåndsmarkere handling som utført.

Påstander merkes «målt», «rapportert av operatør», «beregnet», «tolket» eller
«hypotetisk». Teknisk revisjonsinformasjon kan ligge under «Grunnlag»;
datamangler og antakelser som påvirker beslutningen skal være synlige direkte.

Eksisterende maskintidsanalyse skal ha sin egen forklaring. Nye visninger skal
ikke fremstille dens aktive tidsandel som OEE uten tilgjengelighets-, ytelses-
og kvalitetsgrunnlag for en faktisk definert OEE-beregning.

## 17. Lagring, gjenoppbygging og korreksjoner

Tvillingen skal bruke to tydelig atskilte lagringskategorier:

| Kategori | Innhold | Bevaringsregel |
| --- | --- | --- |
| Primærregistreringer | Definisjonsrevisjoner, datasettregistreringer, kontekst, kildeutdrag/-revisjoner, koblinger, beslutninger, handlinger, resultater, frosset referanseutvalg/policy og audit | Skal sikkerhetskopieres og bevares; kan ikke gjenskapes fra telemetri alene |
| Avledede projeksjoner | Egenskaper, visningsindekser og reproducerbare analyser | Kan gjenskapes når eksakte inngangsrevisjoner og policy fortsatt finnes |

Foreslått plassering er en egen katalog under konfigurert datarot,
`digital_twin/`, med separate SQLite-databaser for `records` og `projections`.
Bruk prosjektets eksisterende SQLite-/migreringsmønster. Ingen ny databaseavhengighet
er nødvendig. Ikke legg registreringene i råtelemetriens JSONL-tre.

En avledet beregning lagrer både innholds-ID og kjøringsmetadata. Den skal
kunne gjentas med samme logiske resultat uten at klokkeslettet for den nye
kjøringen endrer resultatets innholdsfingeravtrykk.

Korreksjon oppretter en ny post/revisjon med referanse til den opprinnelige.
En lesemodell kan vise siste gyldige revisjon, men tidligere vurderinger skal
fortsatt kunne forklare hvilket grunnlag de faktisk brukte. Sletting eller
retensjon må følge en egen besluttet datapolicy; denne planen autoriserer ingen
opprydding, arkivsletting eller ombygging av eksisterende data.

## 18. Foreslått fil- og ansvarsfordeling

```text
catalog/digital_twin/
    contracts.py          # immutable typer, schema og identitetsregler
    policy.py             # versjonerte gyldighets-/egenskapspolicyer
    evidence.py           # datasett- og revisjonsbundne evidensreferanser
    adapters.py           # les eksisterende canonical/operational/legacy-kilder
    snapshots.py          # frys sammenhengende inngangsgrunnlag
    context.py            # prosesskontekst og kontrollerte evidenskoblinger
    features.py           # episodeegenskaper med kildegrunnlag
    comparison.py         # kvalifiserte utvalg, baseline og avståelse
    feedback.py           # beslutning, handling, resultat og oppfølging
    scenarios.py          # avgrensede scenariokontrakter
    records.py            # primærregistreringer og atomisk audit
    projection.py         # gjenskapbare lesemodeller
    tests/

catalog/flask_app/
    process_twin_routes.py
    services/process_twin_service.py
    templates/process_twin.html
    tests/test_process_twin_routes.py
```

Dette er en oppdeling etter ansvar, ikke instruks om å opprette tomme filer.
Hver fase legger til det den faktisk trenger. Ingen ny scheduler, controller,
transportprotokoll, maskinstyring eller separat autorisasjonstjeneste skal
introduseres for å levere denne funksjonen.

## 19. Utviklingsløp og avhengigheter

| Fase | Konkret leveranse | Forutsetning og stoppunkt |
| --- | --- | --- |
| D0: Avklar faktisk grunnlag | Reviderbar kildeinventar, identitets-/klokkestatus, oversikt over tilgjengelige operatør-/utfallsdata, og avstemming av segmenteringskode mot statusdokumenter | Lesende kartlegging; ingen nye produksjonspåstander. D0 skal vise om en pilot faktisk kan føres gjennom |
| D1: Kontrakter og snapshots | Domenetyper, inputbundne referanser og en historisk snapshot over eksisterende kilder | Gjennomgå D0; test blandede datasett, revisjoner, identiteter og tidspunkt |
| D2: Prosesshistorie | Lesende adaptere, tidslinje, episodedetaljer og tjenestevis datakvalitet | Godkjente D1-kontrakter; behold legacy og kanonisk grunnlag atskilt |
| D3: Menneskelig prosesskontekst | Kildeversjoner, foreslåtte/bekreftede koblinger og kontekstregistrering | Menneskelig tilgang, revisjonskonflikter og audit må være verifisert |
| D4: Tilbakemelding | Separate beslutninger, handlingsrapporter, resultater og korreksjoner | Ingen strategi-godkjenning eller maskinutførelse; registrerings-/gjenopprettingstest kreves |
| B0: Faglig sammenligningskontrakt | Avtal egenskaper, nødvendig kontekst, minste utvalg, toleranser og vurderingsmetode på faktisk pilotmateriale | Gjennomgang etter første prosesshistorie; ikke velg terskler fordi de gir pene resultater |
| D5: Egenskaper og referanser | Beregn egenskaper, kvalifiser sammenligningsgrupper, frys baseline-revisjoner | B0; vis avståelse når materialet ikke kvalifiserer |
| D6: Sammenligning og scenario | Forklarbar vurdering og inputbundet eksisterende tidsscenario | D5; kronologisk evaluering uten treningstest-lekkasje |
| D7: Pilot og evaluering | Virkelig prosessforløp med målbar datakvalitet og dokumentert operatøroppfølging | Ingen fullføringspåstand fra bare syntetiske tester eller brukerens aksept av definisjonen |
| Senere: Anbefaling/prediksjon | Egen krav-/gyldighetskontrakt, evaluering og relevante OSL-leveranser | Utenfor denne implementeringsbestillingen |

D1–D4 skal kunne leveres uten å vente på en full statistisk baseline. Det gir
nytte ved å forbinde det vi vet om prosessen, samtidig som nye data kan samles
gjennom legitim bruk. Eksisterende telemetri- og segmenteringsfunksjoner skal
gjenbrukes fremfor å bygges en gang til.

Hver fase skal være en separat, gjennomgåelig endring med egne akseptkriterier.
Dette dokumentet flytter ikke Federation-/OSL-aksept, gjenbruker ikke gamle
releasebevis og starter ikke produksjonstiltak for å få pilotdata.

## 20. Funksjonelle akseptkriterier

«Systemet skal» nedenfor er implementasjonskrav, ikke påstander om eksisterende
beståtte tester.

| ID | Situasjon | Forventet atferd |
| --- | --- | --- |
| PT-01 | Når en tvilling opprettes | Systemet skal kreve identifiserbar faktisk motpart, prosessgrense, mål og valgte egenskaper |
| PT-02 | Når to datasett har like kilde-/instans-/sekvensverdier | Systemet skal bevare separat opprinnelse og aldri slå dem sammen uten eksplisitt avklart kobling |
| PT-03 | Når samme opptak finnes i flere kopier | Systemet skal unngå dobbeltelling eller avstå fra felles referanseutvalg hvis overlapping er uavklart |
| PT-04 | Når to Devices deler en Agent | Systemet skal bevare separat prosesskontekst og oppdage hull på Agent-nivå |
| PT-05 | Når kildeklokken eller legacy-tidssonen er ukjent | Systemet skal vise usikkerheten og avstå fra automatisk tidskobling til menneskehendelser |
| PT-06 | Når det finnes et sekvenshull eller en tidsregresjon | Systemet skal vise bruddet, bevare ukjent tid og unngå arvet kontekst over bruddet |
| PT-07 | Når nye data ankommer etter en snapshot | Systemet skal beholde gamle resultater bundet til deres gamle snapshot |
| PT-08 | Når en kilde- eller notatrevisjon endres | Systemet skal bevare tidligere brukt grunnlag eller rapportere at det ikke lenger er tilgjengelig |
| PT-09 | Når en bruker velger en episode | Systemet skal vise eksisterende segmentklassifikasjon og begrunnelser uten å oppgradere produksjonskandidat til bekreftet del |
| PT-10 | Når flere episoder passer et notat | Systemet skal tilby uavklart/forslag; ingen vilkårlig automatisk bekreftelse |
| PT-11 | Når et støttekort godtas | Systemet skal ikke registrere en utført handling eller positiv effekt automatisk |
| PT-12 | Når en handling er rapportert uten resultat | Systemet skal lagre handlingen og vise `NOT_MEASURED` fremfor å finne på et resultat |
| PT-13 | Når en bruker rapporterer «bedre kvalitet» uten måling | Systemet skal beholde en menneskelig vurdering og ikke generere numerisk forbedring |
| PT-14 | Når en nødvendig kontekstverdi eller enhet mangler | Systemet skal avstå fra kvalifisert baseline-/avvikspåstand og forklare hva som mangler |
| PT-15 | Når en mål-episode også inngår i referanseutvalget | Systemet skal avvise sammenligningen som lekkasje eller bygge en ny eksplisitt referanserevisjon uten den |
| PT-16 | Når en prosentforskjell har null som referansenevner | Systemet skal vise absolutt forskjell og en forklart manglende prosentverdi |
| PT-17 | Når et gyldig tidsscenario velges | Systemet skal gi den eksisterende aritmetiske konsekvensen uten produksjons- eller årsakspåstand |
| PT-18 | Når en viewer forsøker å registrere hendelser | Systemet skal avvise forsøket; lesetilgang gir ingen skriverettighet |
| PT-19 | Når klienten oppgir en annen aktør eller en kilde uten tilgang | Systemet skal avvise eller ignorere forfalsket aktørfelt og nekte uautorisert kildekobling |
| PT-20 | Når samme kommando gjentas eller en gammel revisjon endres | Systemet skal håndtere idempotens og konflikt uten doble eller delvise registreringer |
| PT-21 | Når en projeksjons-/visningsindeks gjenoppbygges | Systemet skal bevare primærregistreringer og produsere samme logiske resultat fra samme inngang og policy |
| PT-22 | Når kilden er utilgjengelig eller replay vises | Systemet skal vise riktig datamodus og status; det skal ikke fremstille gammelt materiale som live |
| PT-23 | Når en API-side bare inneholder en del av resultatet | Systemet skal angi videre sider og bevare stabile totalsummer og snapshot |
| PT-24 | Når en anbefaling, notatstatus eller SysML-fil foreligger | Systemet skal ikke avlede strategi-, maskin- eller Federation-autoritet fra representasjonen |
| PT-25 | Når dokumentasjon, logger, eksport eller deling opprettes | Systemet og leveransen skal holde konfidensielle verdier og metadata utenfor delbare flater; syntetiske eksempler og plassholdere skal være gjennomgått |
| PT-26 | Når brukeren følger en evidensreferanse | Systemet skal kontrollere tilgang til selve kilden og ikke arve videre tilgang fra bare tidslinjen eller en analyse |
| PT-27 | Når en observasjon eller analyse er tilgjengelig | Systemet skal følge definert rolleprofil og ikke avlede en menneskelig beslutning eller utført handling fra dette |
| PT-28 | Når en nødvendig modell er ugyldig eller ikke vurdert | Systemet skal skille modellgyldighet fra teknisk tilgjengelighet og avstå fra vurderinger som krever gyldig modell |
| PT-29 | Når en handling gjelder flere episoder | Systemet skal bevare alle eksplisitte relasjoner og deres kilder uten å tvinge handlingen inn i én episode eller automatisk bekrefte koblinger |

## 21. Verifikasjon og pilotevaluering

Utviklingstestene skal dekke kontraktene over med små anonymiserte datasett,
herunder fler-enhetsstrøm, dupliserte opptak, hull, klokkeusikkerhet,
uavklart kontekst, revisjonsendring og manglende utfallsdata. Syntetiske
handlinger og resultater merkes testdata og skal aldri bli pilotbevis.

Integrasjonstester skal kontrollere lesing fra de eksisterende kanoniske og
operasjonelle kontraktene og at rådata, eksisterende segmenter og notater ikke
endres av analyse. Skriveruter testes for CSRF, tilgang, aktørbinding,
idempotens, transaksjonsbrudd og gjenoppretting. Kjør relevante nye tester på
både Windows og Linux etter repoets eksisterende praksis.

Før en pilot skal en fagperson gjennomgå et frosset utvalg med både normale,
delvise og tvetydige forløp. Utvalgsregler og episode-ID-er registreres før
resultatvurdering. Mål minst:

- andel forløp med sporbar identitet, tid og operasjonskontekst;
- andel påstander med fungerende kildereferanse;
- andel foreslåtte notatkoblinger som fagpersonen bekrefter/avviser;
- hvilke avviksvurderinger fagpersonen er enig/uenig i og hvorfor;
- antall beslutninger, rapporterte handlinger og faktiske resultatregistreringer;
- andel forespørsler der systemet avstår, med årsak;
- oppgaveløsning: kan operatøren finne forløpet, forklare grunnlaget og registrere oppfølging?

Kravet til kildehenvisning er 100 prosent av modellens faktapåstander og
slutninger; brukerens fritekst beholder egen forfatter-/kildereferanse. Andre
effektmål, som ønsket koblingspresisjon eller redusert produksjonstid, fastsettes
før pilot i B0/pilotprotokollen. Det er ikke faglig forsvarlig å velge slike
akseptgrenser etter at resultatene er kjent.

En fungerende sløyfe krever minst én ekte kjede fra kjent prosessituasjon,
via menneskelig beslutning og rapportert handling, til etterfølgende observasjon
og dokumentert resultatvurdering. Dette demonstrerer arbeidsflyten; det er ikke
alene en dokumentert produktivitetsgevinst eller generell modellvalidering.

## 22. Ressursgrenser og drift

Første lesemodell skal sideinndele hendelser og bruke eksisterende indekserte
lagre fremfor å laste hele historien ved hver sidevisning. JSONL-kartlegging og
snapshotbygging skal være avgrensede, eksplisitte operasjoner.

Foreslåtte grenser for første implementasjon er maksimalt 2 GiB JSONL per
datasettkjøring, i tråd med eksisterende tidsscenario, maksimalt 500 rader per
API-side og 64 KiB fritekst per prosessregistrering. Større grunnlag skal gi
tydelig avvisning eller kreve en separat gjennomgått behandling; det skal ikke
stille avkuttes.

Før implementasjon aksepteres, skal D0/D1 velge en fast benchmarkmaskin og et
fast representativt datasett. Logg datamengde, kjøretid, maksimal minnebruk og
spørringslatens. Målbudsjett for første pilot er under to sekunder p95 for en
allerede bygd episodeside på denne referansen; dette er et foreslått mål,
ikke en målt egenskap i dagens produkt. Kostnaden ved første bygg rapporteres separat.

Første løsning arbeider lokalt mot allerede tilgjengelige, autoriserte data.
Ingen skjult opplasting, ny skytjeneste, ekstern modellkall, dependency-
installasjon, automatisk Recorder-konfigurasjon eller maskinstyring inngår.
Datatilførsel og eventuell senere federert deling bruker sine egne etablerte
produktflyter og krever eget oppsett; en tvillingdefinisjon oppretter ingen slik rett.

## 23. Beslutninger før første kodeleveranse

Følgende avklares som D0-resultat, ikke ved å be brukeren gjenta tidligere
godkjenninger eller ved å anta at manglende data finnes:

1. Hvilket faktisk maskin-/prosessomfang og hvilket eksisterende opptak skal
   brukes i den første piloten?
2. Finnes verifiserbare råopptak, kanoniske projeksjoner og operatør-/utfallsdata
   i dette omfanget, og hva mangler?
3. Hva er opprinnelse, maskinidentitet, tidsone, måleenheter og klokkekvalitet?
4. Hvilke eksisterende segmenteringsleveranser er faktisk akseptert på målcommit?
5. Hvem kan registrere definisjon/kontekst, gjennomgå sammenligningspolicy og
   kontrollere pilotresultater gjennom eksisterende menneskelig tilgang?
6. Hvilken første operasjon og hvilket resultat er realistisk å følge opp?

Hvis data for et ledd mangler, skal D0 levere en konkret mangelliste og vise
hvilke tjenester som fortsatt kan leveres. Implementasjon skal ikke blokkere
all historikkvisning bare fordi prediksjon eller full tilbakemelding er uoppnåelig.

## 24. Kilder og eksisterende kontrakter

- [Forskningsgrunnlag og vurdering av eksisterende løsninger](production_process_digital_twin_research.md) — skiller forskningsfunn, standarder og egne designanbefalinger.
- [DarTwin DSL: artikkel og begrepsgrunnlag](https://arxiv.org/html/2510.12478v1).
- [Continuous Evolution of Digital Twins using the DarTwin Notation](https://link.springer.com/article/10.1007/s10270-024-01216-7).
- [DarTwin DSL-kilde](https://github.com/joostm8/DarTwin-DSL/blob/main/DarTrans/DarTwin.sysml). Modellbegreper er kildegrunnlag, ikke et krav om å installere verktøykjeden.
- [Kanoniske observasjoner](../implementation/canonical_mtconnect_observations.md).
- [Operasjonell segmenteringsplan](../implementation/mtconnect_operational_segmentation_plan.md).
- [Eksisterende maskintidsanalyse](../digital_twin.md) og [funksjonsspesifikasjon](digital_twin_what_if_v1.md). Dataprofiler og detaljerte evalueringsinventarer er konfidensielle og gjengis ikke her.
- [Operatørstøttens retning](../fcp_operator_support_plan.md) og [operatørkunnskap i produktet](../operator_strategy_capture.md).
- [Operatørkunnskap, modellering og SysML i FCP](../implementation/operator_knowledge_modeling.md).
- [Menneskelig autentisering og rettigheter](../human-authentication.md).

Denne spesifikasjonen beskriver neste produktforslag. Definisjon og valgt
motpart er godkjent; implementasjonsdetaljer og pilotaksept skal vurderes mot
det konkrete grunnlaget før kode eller produksjonsatferd endres.
