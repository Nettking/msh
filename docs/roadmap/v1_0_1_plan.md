# Forslag til v1.0.1: første leveranse av prosesstvillingen

| Metadata | Verdi |
| --- | --- |
| Status | Implementering bestilt og produktkode etablert lokalt; pilot og releaseaksept gjenstår, se [implementasjonsstatus](v1_0_1_implementation.md) |
| Arbeidsnavn | v1.0.1; endelig publisert versjon avklares etter avsnitt 3 |
| Motpart | Produksjonsprosessen med maskin og operatør |
| Godkjent grunnlag | Definisjonen og valget av faktisk motpart i [utviklerspesifikasjonen](../specs/production_process_digital_twin.md) |
| Omfangsmyndighet | Denne planen foreslår første delmengde av utviklerspesifikasjonen; den gjør ikke hele spesifikasjonen til én release |
| Forskningsgrunnlag | [Forsknings- og løsningsvurderingen](../specs/production_process_digital_twin_research.md) |
| Kravgrunnlag | [Kravspesifikasjon](../specs/production_process_digital_twin_requirements.md); testbare krav og eksplisitt gjenbruk av eksisterende funksjoner |
| Hele tvillingens oppførsel | [F01–F14](../specs/production_process_digital_twin_requirements.md#f01-hva-tvillingen-gjør-fra-start-til-slutt) beskriver også senere saks-/oppfølgingsfunksjoner; denne planen bestemmer bare foreslått første leveranses rekkefølge |
| Valgt oppførsel | [Atferdskontrakt B01–B16](../specs/production_process_digital_twin_behavior.md) presiserer første leveranse og erstatter åpne tekniske alternativer nedenfor |
| Konfidensialitet | Ingen reelle produksjonsidentiteter, datoer, tidsvinduer, verdier, datavolum eller opptaksinventarer i planen |
| Endring nå | Avgrenset historikkimplementasjon med syntetisk verifikasjon; ingen produksjonskilder, grants eller kjørende produksjonstjenester endret |

## 1. Mål og forventet brukerresultat

Eksisterende Recorder-, segmenterings-, DT-, operatør- og tilgangsfunksjoner
er fundamentet. Hver arbeidspakke skal først dokumentere eksisterende dekning
og deretter bare bestille nødvendig integrasjon eller presist avklart gap.
Kravspesifikasjonens gjenbruksmatrise er startpunktet for dette arbeidet.

Første leveranse skal gi en **etterprøvbar historikk for en avgrenset
produksjonsprosess**. En autorisert bruker skal kunne velge et eksisterende
datagrunnlag, undersøke forløpet, se tilgjengelig operatørkunnskap og kontrollere
grunnlaget for hver vist påstand. Manglende identitet, tid, kontekst eller
resultatdata skal være synlig.

Maskin og operatør inngår i samme prosessmodell. Det betyr ikke at telemetri
kan avsløre hvem som arbeidet, hva vedkommende besluttet eller hvorfor en
hendelse oppstod. Slike opplysninger krever egne kilder.

Denne leveransen bygger historikk- og evidensgrunnlaget for den godkjente
digitale tvillingen. Den leverer ikke alene hele tilbakemeldingssløyfen i
definisjonen. Full prosesstvilling krever senere faktisk beslutning, rapportert
handling, ny observasjon og vurdering av resultat.

Et vellykket første brukerforløp er:

1. Brukeren åpner «Prosesstvilling» og ser bare tilgjengelige, autoriserte
   definisjoner og tidligere opprettede snapshots.
2. En bruker med analyserett bestiller et historisk snapshot fra valgte,
   allerede autoriserte kilder og et avgrenset tidsvindu.
3. Brukeren ser tidslinje og datakvalitet. Kanoniske episoder vises når
   grunnlaget finnes; legacy-data vises med sine faktiske begrensninger.
4. En episode eller historikkoppføring kan åpnes med kontekst, kildegrunnlag,
   klassifikasjonsbegrunnelse og eksplisitt ukjent informasjon.
5. Eksisterende menneskelige registreringer vises bare når de kan leses gjennom
   en autorisert lesebane. En kobling til prosessen merkes som bekreftet bare
   når en eksisterende, eksplisitt relasjon dokumenterer det.
6. Brukeren kan gå tilbake til det samme snapshotet og få samme historiske
   resultat, også etter at nye data er kommet til.

## 2. Omfang for første leveranse

### 2.1 Inngår

| Kapabilitet | Konkret leveranse |
| --- | --- |
| Definisjon | Uforanderlig revisjon med faktisk motpart, prosessgrense, maskinbinding, mål, egenskaper og rolleprofil |
| Databinding | Eksplisitte datasett- og kilderevisjoner, avklart opprinnelse og deklarert identitets-/tidskvalitet |
| Historisk snapshot | Eksplisitt bestilling; frosset innhold, sporbare kildegrenser og versjonerte transformasjoner |
| Prosesshistorie | Les eksisterende kanoniske runs, sykluser og episoder; behold klassifikasjoner og begrunnelser |
| Legacy-visning | Eksisterende historiske oppsummeringer med kvalitetsforbehold; ingen oppdiktet kanonisk proveniens |
| Operatørkunnskap | Les allerede eksisterende registreringer og eksplisitte relasjoner, hvis datatilgang og lesekontrakt er tilstrekkelig |
| Evidens | Kildereferanser, påstandstype, revisjon og begrunnelser; ny tilgangskontroll når en referanse åpnes |
| Datakvalitet | Kjent/ukjent tid, kontinuitetsbrudd, uavklart kontekst og tjenestevis tilgjengelighet |
| Web og API | Egen lesende brukerflate; GET leser ferdige snapshots, POST oppretter bare avgrensede analyseartefakter |
| Verifikasjon | Syntetiske kontrakt-, integrasjons-, autorisasjons- og UI-tester; deretter privat historikkpilot |

«Lesende» gjelder kildene og prosesshistorikken. Definisjonsregistrering og
opprettelse av egne snapshots er eksplisitte skriveoperasjoner i DTs eget
lager. De skal ikke skje som følge av GET, kildeåpning eller vanlig navigering.

### 2.2 Utsettes til egne leveranser

- Ny registrering eller korrigering av menneskelig kontekst, koblinger,
  beslutninger, handlingsrapporter og resultater: D3–D4.
- Automatisk forslag til tidskobling av notater. Første leveranse viser
  eksisterende eksplisitte koblinger eller `UNLINKED`.
- Faglig sammenligningskontrakt B0, produksjonsbaseline, avviksberegning,
  effektvurdering, anbefaling og prediksjon: B0 og D5–D6 eller senere.
- Ny prosessimulering, PM4Py, BaSyx/AAS, Ditto, SimPy og FMI-adaptere.
- Live-oppdatering, ny avspillingstjeneste, ny datainnsamling, Federation-deling,
  Recorder-konfigurasjon, OSL-produksjonsimplementasjon og maskinutførelse.
- Generell eksport, delingslenker og eksterne modellkall.

Dagens `/digital-twin` og dens eksisterende aritmetiske tidsscenario beholdes.
Planen bestiller ingen ny scenariomotor og ingen endring av dagens beregning.

## 3. Versjonsnavn og forhold til V1

Brukeren har avsluttet V1-arbeidet. Denne planen starter ikke V1-kontroller,
gamle testløp, deployjobber eller den deaktiverte planlagte oppgaven på nytt.
Den endrer heller ikke historiske akseptbevis eller releasehistorikk.

`v1.0.1` brukes som arbeidsnavn fordi det er navnet på den forespurte neste
planen. Repoets changelog sier at publiserte releaser følger SemVer. Ny,
bakoverkompatibel offentlig funksjonalitet krever normalt en minor-versjon;
en patch er for bakoverkompatible feilrettinger.
[SemVer, reglene 6–7](https://semver.org/#spec-item-6).

Før tagging skal releaseansvarlig derfor velge mellom:

- **Ny DT-funksjonalitet i neste produktrelease:** bruk normalt `v1.1.0`
  dersom endringen utvider produktets offentlige kontrakt.
- **Behold en ren `v1.0.1`-patch:** begrens den releasen til feilrettinger og
  legg den nye prosesstvillingen i en separat minor-release.

Valget skal registreres før releasefrys. Ingen versjonsfil, tag, publisert
kontrakt eller changeloghistorikk omskrives gjennom denne planleveransen.

## 4. Designvalg som følger av forskningen

| Grunnlag | Konsekvens i første leveranse | Hva det ikke dokumenterer |
| --- | --- | --- |
| DarTwin | Bind mål, valgt representasjon og tjenester til en definisjonsrevisjon | Full DSL-konformitet eller effekt i produksjonen |
| Mennesket i sløyfen | Systemet observerer og beskriver; mennesker beslutter og utfører | At en registrert analyse er en beslutning eller utført handling |
| OCEL | Evidensrelasjoner kan gjelde flere prosessobjekter; ingen tvungen én-til-én-kobling | At samtidighet eller overlapp bekrefter årsak |
| W3C PROV | Hold kilde, transformasjon og ansvarlig aktør atskilt | Automatisk kvalitetssikring av innholdet |
| Modellgyldighet | Skill tilgjengelig tjeneste fra faglig gyldig modell og vis avståelse | Generelle normalitetsgrenser eller validert prediksjon |

Dette er prosjektets designvurdering. Kildene, deres begrensninger og vurdering
av alternative plattformer finnes i forskningsnotatet. Første implementasjon
gjenbruker eksisterende Python-, Flask- og SQLite-infrastruktur. Ingen ny
avhengighet eller ny generell tvillingplattform inngår.

## 5. D0: faktisk grunnlag før implementasjon

D0 er en avgrenset kartleggings- og kontraktoppgave. Det skal ikke opprettes
pilotdata gjennom kildeaktivering eller nye opptak for å få planen til å passe.

Følgende skal registreres i et tilgangsstyrt, lokalt D0-resultat:

1. Eksakt valgt kodebase og kilde-/testgrunnlag. Avstem segmenteringskodens
   faktiske kontrakter med eldre statusdokumenter; ikke utled aksept fra at
   filer finnes og ikke åpne V1-aksept på nytt.
2. Hvilket eksisterende materiale piloten kan bruke, med eier-/tilgangsomfang,
   kildeformat, revisjon, tilgjengelighet og integritetskontroll.
3. Binding mellom datasett, source, Agent-instans og Device. Lik visningstekst
   eller like sekvenstall skal ikke gi samme maskinidentitet.
4. Kildeklokke, tidssone, enheter, kontinuitet og forholdet mellom kanonisk
   vannmerke og operasjonell projeksjon.
5. Om operatørregistreringer har lesetilgang, stabil revisjon, kildeproveniens
   og eksplisitte prosessrelasjoner. Manglende ledd registreres som mangler.
6. Hvem som etter eksisterende autorisasjon kan registrere definisjonen,
   bestille analyse og lese hvert datagrunnlag. Ingen nye grants opprettes.
7. Fast referansemiljø, syntetisk testprofil, ressursbudsjett og kriterier for
   den private piloten.

Delbart D0-sammendrag skal bare angi kontraktstatus og manglende kapabiliteter.
Det lokale inventaret med produksjonsmetadata skal ikke legges i Git eller
eksterne tjenester. Alias og innholdshash er fortsatt potensielt sensitive.

### 5.1 D0 avgjør hvilke visninger som kan leveres

| Tilgjengelig grunnlag | Tillatt historikk | Konsekvens for leveransen |
| --- | --- | --- |
| Konsistente kanoniske og operasjonelle revisjoner | Episoder med original klassifikasjon, kontekst og evidens | Primær pilotbane |
| Bare legacy-telemetri | Eksisterende oppsummering og deklarert kildekvalitet | Begrenset delresultat; ikke godkjent kanonisk episodeleveranse |
| Menneskelig registrering uten eksplisitt prosesskobling | Separat `UNLINKED` registrering hvis tilgang og revisjon er kjent | Ingen automatisk kobling |
| Ingen trygt lesbar kilde eller utilstrekkelig identitet | Forklart utilgjengelighet eller uavklart kontekst | Relevant pilotkriterium forblir ikke bestått |

Syntetiske tester kan utvikles mens private datamangler avklares. Manglende
kanonisk pilotgrunnlag skal ikke skjules ved å omdøpe legacy-data til episoder.
En begrenset utgivelse må i så fall få eksplisitt redusert omfang gjennom ny
gjennomgang; den er ikke automatisk ferdig etter denne planen.

## 6. Teknisk gjennomføringskontrakt

### 6.1 Lesebane uten skjulte kildeendringer

Kildegjennomgangen viser at `CanonicalObservationStore` og
`OperationalSegmentationStore` åpner vanlige SQLite-forbindelser og har
initialiserings-/schemalogikk. De skal ikke konstrueres ukritisk i en lesende
DT-rute. Det samme gjelder operatørtjenester som kan opprette startinnhold eller bruke
Federation som følge av en tilsynelatende lokal lesing.

Implementasjonen skal ha en eksplisitt lesekontrakt:

- Åpne bare allerede eksisterende, eksplisitt tillatte kilder; ingen fri
  filsti eller vilkårlig SQL fra en HTTP-klient.
- Valider støttet schema uten å opprette, oppgradere eller reparere kilden.
- Bruk lesetransaksjon og en trygg lesemodus eller en allerede etablert
  konsistent kildekopi. Ikke sett SQLite `immutable` på en aktivt endret base.
- Ikke kopier bare hovedfilen til en aktiv SQLite-base og anta at dette gir
  et konsistent bilde av WAL-innholdet.
- La kildens eksisterende datatilgang styre tillatelsen. Ingen tilkobling skal
  starte opptak, jobber, synkronisering eller endre runtime.
- GET leser den ferdige DT-lesemodellen. Manglende lesemodell gir forklarbar
  status og tilbud om eksplisitt analyse for en bruker med rettighet.

En målrettet utvidelse av eksisterende lagres lesestøtte kan velges fremfor
dupliserte spørringer når dette bevarer opptakernes eksisterende oppførsel.
Valget skal beskrives i den første kodeendringen; generell lagerrefaktor
inngår ikke.

### 6.2 Definisjon og snapshot

Definisjon og datasettbinding registreres gjennom de avgrensede,
beskyttede metadata- og klargjøringsendepunktene i B03–B05/B10.
Rettigheter og request-/responsform er fastsatt der. Ingen anonym bootstrap
eller generell CRUD inngår. Lokal administratorrolle alene gir ikke
datatilgang som brukeren mangler.

En snapshotbygging skal:

1. Kontrollere menneskelig identitet, `analysis.run`, kildenes `data.read`,
   definisjonsrevisjon og tillatt prosess-/datasettomfang.
2. Fryse kilderevisjoner og opptaksgrense før beregning. To databaser er ikke
   automatisk transaksjonelt konsistente; operasjonell projeksjon må kunne
   bindes til det kanoniske vannmerket og transformasjonsrevisjonen.
3. Avvise uavklart revisjonsblanding med konflikt eller eksplisitt status.
   Ikke gjenta bygg automatisk mot et annet grunnlag.
4. Materialisere nødvendige historikkdata og proveniens i DTs eget,
   tilgangsstyrte lager. Rådata og primære menneskeregistreringer beholdes i
   sine eksisterende lagre. Unngå ubegrenset rådataduplisering.
5. Beregne stabil innholdsdigest for snapshotets logiske innhold. Genererte
   objekt-ID-er og vurdering av ferskhet inngår ikke som ustabile fagdata.
6. Publisere snapshotet atomisk når validering, innhold og audit er komplette.
   En avbrutt bygging skal ikke bli en delvis lesbar, gyldig snapshot.

Kanonisk identitet har fortsatt kilde, Agent-instans og sekvens med
datasettopprinnelse som eget navnerom. Device-identitet og Agent-kontinuitet
skal holdes atskilt. Legacy får ikke konstruert Agent-/sekvenshistorikk.

Alle svar bindes til snapshot og revisjoner. Tidspunkt for siste observasjon,
opptak og aktuell ferskhetsvurdering er ulike felt. Første leveranse tillater
bare `HISTORICAL`; andre moduser avvises med en forklart kontraktfeil.

Tidligere snapshots endres ikke når kilder korrigeres. En senere bygging
gir nytt grunnlag. Hvis original evidens ikke lenger kan leses, skal dette
vises eksplisitt; dagens innhold skal ikke erstatte den gamle kilden lydløst.

### 6.3 Menneskelige registreringer og modellstatus

En registrering skal beholde kilde, forfatterreferanse, status og revisjon
uten at DT oppretter nye registreringer eller bekrefter en handling.
Forfatterreferanser skal bare vises når brukeren har tilgang til dem.
Ingen utledning av operatøridentitet fra aktivitet, navn eller tidsmessig
overlapp inngår.

Eksisterende relasjoner bevares med alle objektreferanser og revisjoner.
Tvilsomme eller manglende relasjoner forblir uavklarte. Når kildekontrakten
ikke gir tilstrekkelig lesestøtte, skal visningen forklare dette i stedet
for å opprette startinnhold, skrive eller hente via Federation automatisk.

Modelltilgjengelighet og `ModelValidityAssessment` vises separat.
`NOT_ASSESSED` må ikke bli «validert» bare fordi data kan leses.
Faktabeskrivelser kan vises med kilde, mens tjenester som krever faglig
validering avstår. Ingen samlet grønn indikator skal skjule manglende ledd.

### 6.4 Avgrenset API

Atferdskontraktens B10 har det fullstendige, presiserte rutekartet, inkludert
registreringsinngang, kildeklargjøring, kommandostatus og eksplisitt
gjenoppbygging. Tabellen nedenfor viser historikkens lesekjerne og skal
ikke brukes som et alternativt eller uttømmende API-forslag.

Alle ruter nedenfor er planlagte. Prefiks: `/api/process-twins/v1`.

| Rute | Tillatt operasjon i denne leveransen |
| --- | --- |
| `GET /twins` | Autoriserte definisjoner og tjenestestatus |
| `GET /twins/{id}/revisions/{revision}` | Én uforanderlig definisjon |
| `POST /twins/{id}/snapshots` | Historisk analyseartefakt etter eksplisitt bestilling |
| `GET /snapshots/{id}` | Omfang, revisjoner, datamodus og kvalitet |
| `GET /snapshots/{id}/timeline` | Stabil, sideinndelt historikk |
| `GET /snapshots/{id}/episodes/{episode_id}` | Eksakt episode med kontekst og evidens |
| `GET /snapshots/{id}/links` | Eksisterende, autoriserte relasjoner; ingen nye koblingsforslag |

Evidensoppslag går gjennom den konkrete ruten i B08/B10 og serverens
autoriserte kildeoppslag. Ingen direkte
filsti, usignert råfil-URL eller klientstyrt kildeforespørsel skal brukes.
Ruten har samme tilgangskontroll på hvert oppslag.

GET krever `data.read` og objektets datatilgang; POST krever i tillegg
`analysis.run`, CSRF, serverbundet aktør og `Idempotency-Key`. Samme nøkkel,
aktør og parametere gir samme resultat; endret innhold gir `409`.
Ingen nye standardrettigheter eller standardgrants inngår.

Snapshotbestillingen bruker hovedspesifikasjonens request-kontrakt. Følsomme
tidsvinduer og kildevalg sendes i body; URL og logg skal ikke inneholde dem.
Historikkfiltre uttrykkes med ugjennomsiktige snapshotbundne referanser og
cursorer. Cursor er ikke en tilgangsrett og skal ikke kode lesbar fritekst,
identitet eller tidsmetadata.

Lister bruker standard `limit=100`, maksimalt `500`, stabil sortering og
`has_more`. Ukjente/ikke-autoriserte objekter håndteres uten innhold som
avslører om en annen brukers data finnes. Svarreglene `400`, `401`/`403`,
`404`, `409`, `413`, `422` og `503` følger hovedspesifikasjonen.

### 6.5 Filgrenser

| Område | Ansvar i første leveranse |
| --- | --- |
| `catalog/digital_twin/contracts.py` | Bare nødvendige definisjons-, snapshot-, referanse-, kvalitets- og relasjonstyper |
| `catalog/digital_twin/adapters.py` | Autorisert lesing av eksisterende canonical/operational/legacy-kontrakter |
| `catalog/digital_twin/evidence.py` | Revisjonsbinding og kontrollert evidensoppslag |
| `catalog/digital_twin/snapshots.py` | Konsistenskontroll og avgrenset bygging |
| `catalog/digital_twin/records.py` | Egne definisjons-/snapshotmanifest, idempotens og atomisk audit |
| `catalog/digital_twin/projection.py` | Gjenskapbar, snapshotbundet historikkmodell |
| `catalog/flask_app/services/process_twin_service.py` | Applikasjonskommandoer og tilgangs-/omfangsvalidering |
| `catalog/flask_app/process_twin_routes.py` | Ny blueprint, kontrollert request/response og sidevisning |
| `catalog/flask_app/templates/process_twin.html` | Historikk, episodedetaljer, evidens og forklarte tomtilstander |
| Eksisterende sentral auth-policy | Eksplisitt registrering av de nye endepunktene |

Tabellen angir ansvarsgrenser, ikke en bestilling av nye parallelle moduler.
Vurder eksisterende eiermoduler, datatyper, DT-snapshot/cache og tester først.
Opprett bare filer som et dokumentert gap i den konkrete leveransen krever.
Grensen mot Recorder-lagrene kan kreve en liten lesestøtteendring med egne tester.
Primære definisjons-/snapshotmanifest og gjenskapbar projeksjon skal ha ulike
ansvar. Tap av en visningsindeks skal ikke slette primærregistreringer.

## 7. Gjennomførbare arbeidspakker

Hver pakke skal kunne gjennomgås som en avgrenset endring. Ingen pakke har
kalenderestimat før D0 har avklart grunnlaget og lesegrensene.

| Pakke | Avhenger av | Arbeid og konkret gjennomgangsresultat | Ferdigkriterium |
| --- | --- | --- | --- |
| DT0 — D0 og kontraktbeslutninger | Gjennomgått planomfang | Privat inventar, offentlig struktur uten produksjonsmetadata, valgt lesebane og pilotprotokoll | Hver kilde har eksplisitt status; ingen skjulte datatilgangs-/identitetsforutsetninger |
| DT1 — Domenekontrakter | DT0 | Typene i første delmengde, deterministisk serialisering, identitets-/tidsregler og syntetiske fixtures | Kollisjon, ukjent tid og revisjonsblanding har entydig håndtering |
| DT2 — Kildelesing og snapshotlager | DT1 | Målrettede leseadaptere, konsistente kildegrenser, atomisk snapshot, idempotens og gjenoppbygging | Kilder er uendret; lik inngang/policy gir likt logisk historikkinnhold |
| DT3 — Applikasjon og API | DT2 | Minimumsrutene, sentral policy, kilde-/objektkontroll, paging og kontrollerte feilsvar | Rettighets- og konfliktmatrisen består med syntetiske data |
| DT4 — Brukerflate | DT3 | Historisk oversikt, episode-/evidenspanel, eksisterende notater og forklarte mangler | Hele første brukerforløp kan gjennomføres uten falske påstander eller skjulte bygginger |
| DT5 — Privat historikkpilot | DT4 og reelt pilotgrunnlag | Frosset utvalg, operatør/faglig gjennomgang og avviksliste i tilgangsstyrt miljø | Kravene R01–R12 under er faktisk vurdert; mangler og feil er bevart |
| DT6 — Releaseklargjøring | DT5 | Valgt versjonsnavn, oppdatert brukerdokumentasjon, avgrenset releasebeskrivelse og kandidatsjekker | Omfang og faktiske test-/pilotresultater kan gjennomgås på én eksakt kandidat |

### 7.1 Den første implementasjonsoppgaven etter planbehandling

Start med **DT0: verifiser lesekontrakten og frys første datagrense**.
Utvikleren skal levere en gjennomgåelig beslutning som svarer på:

- Hvilke eksisterende API-er kan lese canonical, operational og eventuelle
  menneskeregistreringer uten initialisering, migrering, nytt startinnhold eller nettverk?
- Hvilken liten adapter-/lagerendring er nødvendig der dette ikke finnes?
- Hvordan bevises at segmenteringen svarer til samme kanoniske vannmerke?
- Hvilke visninger støttes av reelle kilder, og hvilke må vise mangler?
- Hvordan opprettes definisjonen av en legitim menneskelig aktør uten å
  åpne en ny privilegert bootstrapbane?
- Hvor lagres nye, konfidensielle DT-artefakter gjennom eksisterende
  applikasjonslagring, og hvilke grenser gjelder før neste pakke starter?

Resultatet er en presis implementasjonskontrakt og små syntetiske fixtures,
ikke en ny controller, datainnsamler eller full ombygging av datalageret.
Privat datakartlegging skal ikke kopieres inn i PR-beskrivelsen.

## 8. Tester og aksept for den avgrensede releasen

Alle nye tester bruker håndskrevne syntetiske data. Ekte materiale brukes
bare i den private piloten. Testdata skal ikke avledes fra reelle forløp ved
bare å bytte navn eller flytte datoer.

| Krav | Test eller gjennomgang | Aksept |
| --- | --- | --- |
| R01 — Reell avgrensning | Opprett definisjon uten motpart, mål eller maskin-/prosessgrense | Avvis; ingen ufullstendig gyldig definisjon |
| R02 — Identitet | Like source/instans/sekvens i ulike datasett; flere Devices i samme Agent | Ingen utilsiktet sammenslåing, dobbeltelling eller arvet Device-kontekst |
| R03 — Tid og brudd | Ukjent legacy-tidssone, sekvenshull og tidsregresjon | Ukjentgrunnlag og brudd vises; ingen automatisk menneskekobling |
| R04 — Kildevern | Les fra skrivebeskyttet, manglende og schema-avvikende syntetisk kilde | Ingen kildeopprettelse, migrering eller skjult fallback; korrekt status |
| R05 — Konsistens | Nye observasjoner under bygging, uforenlig projeksjonsvannmerke og korrigert kilde | Ett konsistent grunnlag eller eksplisitt avvisning; gammelt snapshot er uendret |
| R06 — Atomisitet | Avbrutt bygging, gjentatt bestilling og gjenbrukt nøkkel med annet innhold | Ingen delvis publisering; samme resultat eller forklart konflikt |
| R07 — Påstanders betydning | Produksjonskandidat, godkjent støttekort og notat uten resultat | Ingen automatisk deltelling, handling, årsak eller effekt |
| R08 — Evidens og rettighet | Kryssbruker-/datasettforespørsel, tilbakekalt datatilgang og uautorisert evidensoppslag | Ingen datautlevering gjennom snapshot, lenke, cursor eller mellomlager |
| R09 — Lesestabilitet | Flere sider, gjenåpnet snapshot og gjenoppbygd visningsindeks | Samme logiske innhold og totalsummer; ingen bortfalte eller doble oppføringer |
| R10 — Konfidensialitet | Request-/feillogg, URL, audit, UI-escaping, dokumentasjon og testartefakter | Ingen følsom payload i delbare flater; privat audit har eksisterende tilgangsvern |
| R11 — Brukerflyt og grenser | Historisk modus, utilgjengelig kilde, manglende notater, avvist LIVE og ukjent modellgyldighet | Brukeren ser hva som finnes og hva som mangler; ingen falsk live-/validert-status |
| R12 — Ressurser og kompatibilitet | Avgrenset bygg og paging på fast testprofil; dagens `/digital-twin`-kontrakt | Dokumentert ressursbruk, tydelig grensefeil, ingen endret eksisterende beregning |

For hver kildeadapter testes at snapshotbygging ikke skriver til
primærkilden. GET testes dessuten for at det ikke bygger, oppretter startinnhold, migrerer,
kaller eksterne tjenester eller oppretter analyseartefakter. Appens normale
autentiserings-/tilgangsaudit følger eksisterende kontrakt.

Rettighetstester dekker manglende sign-in, viewer med lesetilgang, bruker med
analyserett uten kildetilgang og legitim analysebruker. De dekker også
serverbundet aktør, CSRF og begrenset objektomfang. Gjettbar objekt-ID eller
kjent snapshot-ID skal aldri være tilstrekkelig adgang.

### 8.1 Kobling til hovedspesifikasjonens akseptkriterier

| Dekning | PT-kriterier | Påstand denne releasen kan gjøre |
| --- | --- | --- |
| Direkte del av første leveranse | PT-01–PT-09, PT-21–PT-23, PT-25–PT-26 | Kan bestås for definisjon, historisk snapshot, lesing, kilder og datakvalitet innen valgt omfang |
| Avgrenset kontrakt-/negativtest | PT-10–PT-11, PT-18–PT-20, PT-24, PT-27–PT-29 | Ingen utilsiktet kobling/utførelse/grant; idempotens bare for snapshotkommandoen; eksisterende relasjoner kan leses |
| Utsatt produktatferd | PT-12–PT-16 og de nye skrive-/analyseflytene i de delvis dekkede kriteriene | Ingen PASS-påstand før de relevante senere fasene er implementert og evaluert |
| Eksisterende funksjon, ingen ny leveranse | PT-17 | Dagens aritmetiske scenario bevares; ikke ny prosessimulering |

Et delvis dekket PT-kriterium skal ikke markeres fullt bestått i
hovedspesifikasjonen. Eksempelvis beviser lesing av flere eksisterende
relasjoner ikke at ny handlingsregistrering på tvers av episoder fungerer.

### 8.2 Privat pilot og beslutningsregel

Pilotprotokollen velger på forhånd normale, delvise og tvetydige historikkforløp.
Utvalgets faktiske referanser og produksjonsmetadata forblir private.
En operatør eller fagperson skal kunne:

1. Finne et forløp og forklare hvilke deler som er målt, rapportert og tolket.
2. Åpne evidensen for alle viste fakta/slutninger som vedkommende har tilgang
   til, eller få en korrekt markering når en kilde er utilgjengelig.
3. Identifisere brudd, ukjent tid og manglende operatør-/resultatgrunnlag.
4. Gjenåpne samme snapshot og kjenne igjen samme historikk.

Alle genererte faktapåstander og slutninger skal ha kilde-/metodereferanse;
mennesketekst skal beholde egen kilderevisjon. Dette er et fullstendighetskrav,
ikke bevis for at enhver sensorverdi er sann eller at modellen er faglig validert.

Piloten må dokumentere at maskinforløpet kan rekonstrueres innen pilotens
erklærte kvalitet. For målsettingen om maskin **og** operatør kreves også
minst én ekte, sporbar menneskeregistrering. Manglende kobling kan være et
gyldig pilotfunn, men skal ikke presenteres som en demonstrert samlet prosesskjede.

Feil registreres med bevart inngangsgrunnlag og forventet/faktisk oppførsel
inne i det private miljøet. Syntetisk kopi av selve feilmekanismen kan brukes
som regresjonstest; privat payload skal ikke kopieres til testen.

## 9. Ressurser, lagring og kompatibilitet

Første bygging er en eksplisitt, avgrenset lokal operasjon. Ingen ny scheduler
eller kontinuerlig prosess introduseres. Overskrides det vedtatte synkrone
budsjettet, skal forespørselen avvises med forklart grense; ny jobbmotor er
ikke en automatisk utvidelse av denne planen.

Atferdskontraktens B02/B15 fastsetter inngangsgrense, sidegrense, byggefrist,
minne-/artefaktbudsjett og ytelsesoppskrift. DT0 verifiserer tilkobling til
eksisterende kontrakter og velger reelt pilotgrunnlag; pakken velger ikke
alternative null-/feilregler eller ubegrensede budsjetter. Tallene er tekniske
produktforslag, ingen påstand om nåværende produksjonsvolum eller ytelse.

GET skal ikke lese hele råhistorien på nytt. Store uttrekk avvises eller
sideinndeles; ingen stille avkutting. Artefaktstørrelse, feil og ressursbruk
måles lokalt uten sensitive etiketter i eksterne metrikker.

Eksisterende Recorder-/Federation-lagring, rådata og menneskeregistreringer
skal ikke migreres av DT. Nye artefakter har eksplisitt schema og lagringsbane
i eksisterende applikasjonslagring. Ingen automatisk sletting eller retention-
opprydding inngår. Manglende plass håndteres før bygging og ved kontrollert
feil; eksisterende data skal bevares.

Ved tilbakeføring skal ny rute kunne tas ut av bruk gjennom vanlig
produktoppdatering mens tidligere kilder og nye primærmanifest beholdes.
Eldre produkt må ikke forsøke å reparere eller nedgradere et ukjent DT-schema.
Kandidatgjennomgangen skal beskrive dette før installasjon.

## 10. Releasekriterier og neste faser

Før publisering av den avgrensede funksjonen kreves:

- Gjennomgått første omfang, valgt releaseversjon og alle DT0-beslutninger.
- R01–R12 med reelle resultater på eksakt kandidat; nye relevante tester på
  Windows og Linux gjennom repoets eksisterende praksis.
- Relevante permanente produktkontroller som repoet krever for endringen.
  Ingen manuell repetisjon av allerede grønne suiter uten ny begrunnelse.
- Privat historikkpilot med bevart avviksliste; ingen kjent kritisk
  datatilgangs-, kildeendrings-, identitets- eller konsistensfeil.
- Brukerdokumentasjon som beskriver historisk datamodus, kildekrav,
  tilgang, begrensninger og opprettelse av analyseartefakter.
- Gjennomgåtte diff-, logg-, test- og demoartefakter uten produksjonsmetadata
  på delbare flater. Dette er ikke en erklæring om at eldre repo/historikk
  allerede er anonymisert.
- Releasebeskrivelse som sier «historikk- og evidensgrunnlag for
  prosesstvillingen» og presist angir hva som er utsatt.

Denne planen tillater ikke at syntetiske tester erstatter pilotbevis eller
at dokumentasjon alene blir en funksjons-/produksjonsaksept. Kodearbeid,
commit, push, release og produksjonsaktivering er ikke utført gjennom
planleveransen.

Etter første historikkleveranse foreslås følgende rekkefølge:

1. **D3:** eksplisitt menneskelig kontekst og gjennomgåtte evidenskoblinger.
2. **D4:** separat beslutning, handlingsrapport, resultat og korreksjon.
3. **B0:** faglig sammenlignbarhet og forhåndsvalgte evalueringsregler.
4. **D5–D6:** kvalifisert historisk sammenligning og avgrensede scenarioer.
5. **D7:** ekte oppfølging gjennom hele tilbakemeldingssløyfen.
6. Senere: dokumentert behov for prosess mining, simulering eller prediksjon
   før valg av ny plattform eller avhengighet.

Ingen framtidig versjon får ferdigstatus eller kalenderløfte av denne listen.
Den bestilte lokale implementasjonen og de faktiske testresultatene er
beskrevet i [implementasjonsstatus](v1_0_1_implementation.md). Reelt
pilotgrunnlag finnes ennå ikke; DT0s private kildebinding og DT5 følger
[pilotprotokollen](v1_0_1_pilot_acceptance.md) når materialet foreligger.
