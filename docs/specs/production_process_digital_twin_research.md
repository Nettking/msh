# Forskningsgrunnlag og løsningsvalg for prosesstvillingen

| Metadata | Verdi |
| --- | --- |
| Status | Kildebasert vurdering og forslag til design; ingen produktimplementasjon eller bibliotekutprøving |
| Motpart | Produksjonsprosessen med maskin og operatør, som godkjent av eier |
| Hovedspesifikasjon | [Prosesstvillingens utviklerspesifikasjon](production_process_digital_twin.md) |
| Kravgrunnlag | [Kravspesifikasjon med gjenbruk og verifikasjon](production_process_digital_twin_requirements.md) |
| Kilder | Offentlige forskningsartikler, standardbeskrivelser og offisiell prosjektdokumentasjon |
| Konfidensialitet | Ingen produksjonsverdier, kildeidentiteter, produksjonsdatoer, datavolum eller detaljerte opptaksinventarer |
| Versjonsbinding | [Nytt planforslag med arbeidsnavn v1.0.1](../roadmap/v1_0_1_plan.md) avgrenser første leveranse; omfang og publisert versjonsnavn er fortsatt forslag |

## 1. Anbefaling

Bygg den første prosesstvillingen videre på FCPs eksisterende telemetri,
operasjonelle segmentering og menneskelige tilgangskontroll. Bruk forskning og
standarder til å presisere modellgrensene og datakontraktene.

Anbefalt kombinasjon er:

1. DarTwin for sammenhengen mellom formål, mål, tvillingmoduler og forbindelser.
2. En eksplisitt fordeling mellom maskinell observasjon/analyse og menneskelig
   beslutning/utførelse, støttet av forskning på mennesker i tvillingsystemer.
3. Relasjoner inspirert av OCEL for hendelser som gjelder flere prosessobjekter.
4. Et proveniensbegrepsapparat som kan kartlegges til W3C PROV.
5. Tjenestespesifikk validering med erklært gyldighetsområde og avståelse.

Min vurdering er at dette treffer det første produktbehovet bedre enn å innføre
en ny generell tvillingplattform. Plattformbytte, grafdatabase og ny simulator
er egne løsningsvalg som først bør tas når et konkret integrasjons- eller
simuleringsbehov er dokumentert.

## 2. Metode og styrke på grunnlaget

Dette er en målrettet kildegjennomgang, ikke en uttømmende systematisk
litteraturstudie. Den dekker operatørinvolvering, tvillingers formål og utvikling,
prosesshistorie, proveniens, modellvalidering og relevante programvarekomponenter.

Det ble brukt generelle faglige søkeord. Konfidensielt materiale inngikk ikke i
eksterne søk, tjenestekall eller demonstrasjoner. Vurderingen har ikke kjørt
produktbenchmark, installert rammeverk eller sendt prosjektdata til leverandører.

Kildene brukes forskjellig:

- **Konseptuell forskning** begrunner begreper og arbeidsdeling, men dokumenterer
  ikke produksjonseffekt i vår prosess.
- **Empirisk metodeforskning** viser metoder i et bestemt forsøk. Terskler og
  feilrater kan ikke overføres direkte til en annen maskin eller prosess.
- **Standarder** beskriver representasjon og interoperabilitet. En mapping er
  ikke i seg selv validering av datainnhold eller faglig betydning.
- **Prosjektdokumentasjon** beskriver tilgjengelige funksjoner. Den er ikke
  dokumentasjon av at et produkt er egnet, integrert eller testet hos oss.

Anbefalingene nedenfor er prosjektets vurderinger av disse kildene, og er ikke
anbefalinger gitt av forskerne eller leverandørene om FCP.

## 3. Forskningsfunn og konsekvenser

### R1. Mennesket inngår i produksjonsarkitekturen

Kim mfl. foreslår en arkitektur der digitale, fysiske og menneskelige deler
inngår i et samlet produksjonssystem. Det støtter at operatørens informasjon og
beslutninger modelleres som egne bidrag, fremfor å behandles som manglende
automatisering. Grunnlaget som er gjennomgått her er artikkelens offisielle
NIST-sammendrag; dette er et konseptuelt arkitekturbidrag.
[NIST: Conceptual Architecture of Digital Twins with Human-in-the-Loop](https://www.nist.gov/publications/conceptual-architecture-digital-twins-human-loop-based-smart-manufacturing).

**Konsekvens for oss:** Et forløp skal kunne inneholde målinger, en menneskelig
observasjon, en beslutning, en rapportert handling og et resultat uten at de
sammenblandes. Operatøren er en aktør i prosessen; funksjonen krever ingen
modell av personens egenskaper eller individuell produktivitet.

### R2. Automatiseringsgrad må velges per rolle

Agrawal mfl. skiller mellom observatør, analytiker, beslutningstaker og utfører,
med ulik automatiseringsgrad for hver rolle. Det gjennomgåtte forfattersammendraget
oppgir et konseptuelt rammeverk med en illustrerende case, ikke en universell
automatiseringsstige som alle tvillinger må følge.
[Digital Twin: Where do humans fit in?](https://arxiv.org/abs/2301.03040).

**Konsekvens for oss:** Første rolleprofil skal være eksplisitt: systemet
observerer og beregner avgrensede beskrivelser; mennesket vurderer, beslutter og
utfører eventuelle tiltak. Et godkjent støttekort kan derfor ikke automatisk
registreres som en utført maskinhandling.

### R3. Mål, struktur og videreutvikling henger sammen

DarTwin DSL formaliserer mål og tvillingarkitektur i SysML v2 og beskriver
utvikling av tvillingsystemer. Den tidligere DarTwin-artikkelen behandler også
hvordan formål, egenskaper og sammensetning utvikler seg. Definisjons- og
arkitekturdelene er relevante her; dette er ingen ferdig kjøremotor for våre
målinger eller operatørhendelser.
[DarTwin DSL](https://arxiv.org/html/2510.12478v1),
[DarTwin notation](https://link.springer.com/article/10.1007/s10270-024-01216-7).

**Konsekvens for oss:** Behold en versjonert tvillingdefinisjon med mål,
egenskaper, rolleprofil, tjenester og forbindelser. En ny visualisering eller
eksportfil er en representasjon av denne modellen. Det kreves ikke en SysML-
verktøykjede for å levere den første prosesshistorien.

### R4. Modellens gyldighet må undersøkes under bruk

Mertens og Denil undersøker kontinuerlig validering ved å sammenligne en modell
med systemobservasjoner. Deres kran-case viser at ulike metrikker kan reagere
forskjellig på ulike avvik, og at terskelvalg fortsatt krever vurdering.
Gjennomgåtte deler er modellgyldighet, metode, evaluering og begrensninger.
Dette er et spesifikt eksperiment, ikke en ferdig toleransemodell for oss.
[Reusing Model Validation Methods for the Continuous Validation of Digital Twins](https://arxiv.org/html/2512.04117v1).

**Konsekvens for oss:** Registrer hvilken modellrevisjon, metrikk, toleranse og
hvilket gyldighetsområde en vurdering gjelder. Et avvik kan skyldes prosessen,
modellen, konteksten eller datakvaliteten. Systemet skal ikke automatisk stille
en feildiagnose eller tilpasse modellen slik at et avvik blir borte.

### R5. Programvareverifikasjon og faglig validering er ulike oppgaver

Shao, Hightower og Schindel fremhever verifikasjon, validering og
usikkerhetskvantifisering gjennom tvillingens livsløp. Det offisielle NIST-
sammendraget knytter troverdighet til tvillingens formål og mål.
[Credibility Consideration for Digital Twins in Manufacturing](https://www.nist.gov/publications/credibility-consideration-digital-twins-manufacturing).

**Konsekvens for oss:** Ha tre separate rapporter: korrekt programvareatferd,
gyldig prosess-/modellfortolkning og faktisk nytte i operatørens arbeidsflyt.
Grønne enhetstester dokumenterer bare den første. Ingen terskel for et minste
antall episoder blir et forskningsresultat ved å stå i spesifikasjonen.

### R6. Prosessutvinning er relevant når hendelsene har riktig betydning

Lugaresis konferanseartikkel behandler prosessutvinning som støtte for
modellgenerering, sporprofilering og ytelsesanalyse i produksjonstvillinger,
og diskuterer åpne utfordringer. Dette er en forskningsoversikt og problem-
analyse, ikke en demonstrasjon av at tilfeldig telemetri kan tolkes automatisk.
[Process Mining as Catalyst of Digital Twins for Production Systems](https://informs-sim.org/wsc24papers/con213.pdf).

**Konsekvens for oss:** Lag først meningsfulle hendelser og forbindelser fra
den eksisterende operasjonelle lagdelingen. En måleverdi er ikke automatisk en
aktivitet, og et sekvensnummer er ikke automatisk en prosessinstans.

## 4. Relevante standarder for datamodellen

### S1. OCEL: flere objekter og relasjoner per hendelse

OCEL beskriver objektorienterte hendelseslogger med hendelser, objekter,
objektattributter og kvalifiserte relasjoner. OCEL 2.0 beskriver JSON-, XML- og
SQLite-formater; nettsiden omtaler også en senere revisjon. En eventuell adapter
skal velge og teste én eksakt versjon.
[OCEL-konsepter](https://www.ocel-standard.org/),
[spesifikasjonsoversikt](https://www.ocel-standard.org/specification/overview/).

Foreslått lokal bruk av prinsippet:

| Prosessobjekt | Hendelse | Mulig relasjon |
| --- | --- | --- |
| Episode | Maskintilstand endres | `observed_in` |
| Episode og operasjonskontekst | Beslutning registreres | `concerns` |
| Flere episoder | Handling rapporteres | `reported_for` |
| Handling og måleobjekt | Resultat registreres | `evaluates` |

Relasjonsnavnene ovenfor er våre foreslåtte begreper, ikke standardens faste
domeneord. En relasjon bærer kilde, revisjon og status. Manglende kobling skal
forbli ukjent. Vi trenger verken OCEL-import/eksport eller en ny pakke for å
bevare en slik mange-til-mange-modell i første leveranse.

### S2. W3C PROV: kilde, bearbeiding og ansvar

PROV skiller mellom Entity, Activity og Agent og relasjoner for blant annet
bruk, avledning og attribusjon. Dette gir et etablert språk for proveniens;
det etablerer ikke sannhet, kausal effekt eller tilgangsrettigheter.
[W3C PROV-O](https://www.w3.org/TR/prov-o/).

Vår mapping foreslås slik:

| PROV-begrep | Vårt begrep |
| --- | --- |
| Entity | Eksakt datakilderevisjon, snapshot, notatrevisjon eller analyseresultat |
| Activity | En bestemt projeksjon, sammenligningskjøring eller menneskelig registrering |
| Agent | Tilgangsstyrt programvare-/menneskeaktør med definert rolle |
| `used` / `wasDerivedFrom` | Hvilke konkrete kilde- og policyrevisjoner resultatet bygger på |
| `wasGeneratedBy` / `wasAssociatedWith` | Hvilken bearbeiding som laget resultatet og hvem som var ansvarlig |

Dette kan først uttrykkes i eksisterende JSON-/SQLite-kontrakter. RDF, en
trippeldatabase og standardkonform eksport er senere, separate adaptervalg.

## 5. Vurdering av eksisterende programvareløsninger

Tabellen skiller kildebeskrevne funksjoner fra vår vurdering av egnethet.
Ingen av verktøyene er installert eller prøvd ut som del av denne gjennomgangen.

| Løsning | Dokumentert rolle | Vår vurdering |
| --- | --- | --- |
| Eksisterende FCP | Lokal telemetri, kanoniske observasjoner, operasjonell lagdeling, notater og tilgangskontroll | Anbefalt kjerne for første leveranse; undersøk målcommit og eksisterende kontrakter før ny kode |
| PM4Py | Prosessoppdagelse, samsvarskontroll og objektorientert prosessutvinning | Relevant senere analysekomponent når hendelses-/objektkoblinger er pålitelige; ikke første infrastrukturvalg |
| Eclipse BaSyx / AAS | Identitet og delmodeller for interoperabel tilgang til egenskaper og tjenester | Relevant hvis vi trenger AAS-utveksling med andre systemer; løser ikke alene operatørfortolkning og validert atferdsmodell |
| Eclipse Ditto | Things, features, policies, konnektivitet og historiske entitetsrevisjoner | Relevant ved separat behov for en delt tilstandstjeneste; vil kreve avklaring mot eksisterende FCP-ansvar |
| SimPy | Diskret hendelsessimulering med prosesser og delte ressurser | Relevant for senere kø-, ressurs- eller flytscenarioer når modellen og parameterne kan valideres |
| FMI / FMU | Grensesnitt for utveksling og kobling av dynamiske simuleringsmodeller | Relevant når det finnes en faktisk modell som skal kobles inn; ingen grunn til å innføre dette for ren prosesshistorie |

### PM4Py

Offisiell dokumentasjon beskriver prosessoppdagelse, samsvarskontroll,
ytelsesanalyse og objektorientert analyse. Community-utgaven oppgis under AGPL,
med en separat kommersiell lisensmulighet. Lisenskompatibilitet må avklares for
tiltenkt bruk før eventuell avhengighet velges.
[Funksjoner og lisens](https://processintelligence.solutions/pm4py/).

Anbefalingen er å bevare en adaptergrense. Et senere, avgrenset forsøk kan
undersøke en konkret samsvarskontroll på et syntetisk og deretter autorisert
lokalt hendelsesutvalg. Biblioteket erstatter ikke domenearbeidet som avgjør
hva en episode, operatørhandling eller prosessrelasjon faktisk betyr.

### Eclipse BaSyx og Asset Administration Shell

BaSyx beskriver AAS som grunnlag for interoperabel tilgang til eiendeler,
prosesser og deres delmodeller. Tidsserier kan ligge internt, i filer eller i
en lenket tidsseriedatabase; sistnevnte innebærer ytterligere datainfrastruktur.
[BaSyx-konsepter](https://eclipse.dev/basyx/concepts/),
[tidsserier](https://www.basyx.org/get-started/behaviour/time-series).

For oss er dette et mulig framtidig utvekslingsformat. En AAS-struktur gir ikke
automatisk kunnskap om operatørens beslutninger eller dokumentert prosesseffekt.
En adapter bør derfor komme etter at den lokale prosesskontrakten er stabil.

### Eclipse Ditto

Ditto tilbyr entiteter, tilgangspolicyer og konnektivitet. Dagens dokumentasjon
beskriver historiske revisjoner og hendelsesstrømmer, og sier uttrykkelig at
historikken ikke tilsvarer en tidsseriedatabase med aggregering eller komprimering.
[Ditto-oversikt](https://eclipse.dev/ditto/intro-overview.html),
[historikk](https://eclipse.dev/ditto/basic-history.html).

Det ville være feil å begrunne et avslag med at Ditto mangler historikk.
Vår grunn til å utsette det er at en slik tjeneste ikke i seg selv leverer
prosesssemantikken og vil gi et nytt integrasjonspunkt. Ved et senere valg må
en utgitt versjon bindes; deler av dokumentasjonen gjelder utviklingsversjonen.

### SimPy og FMI

SimPy støtter diskrete hendelsesprosesser og delte ressurser og oppgis med
MIT-lisens. Det kan brukes til simulerte køer eller ressurskonflikter.
[SimPy-dokumentasjon](https://simpy.readthedocs.io/en/stable/).

FMI definerer modellutveksling, co-simulering og scheduled execution. Det
beskriver grensesnittet, mens en koblet modell fortsatt trenger et gyldig
bruksområde og en egnet gjennomføringsalgoritme.
[FMI-spesifikasjonen](https://fmi-standard.org/docs/main/).

Vår vurdering er å utsette begge. En ren tidsomfordeling krever ingen ny
simulator. En reell simulering av produksjonsflyt krever først validerte
aktiviteter, ressurser, forbindelser, forutsetninger og evalueringsmål.

## 6. Konkret konsekvens for utviklerspesifikasjonen

Følgende presiseringer tas inn i hovedspesifikasjonen:

1. **Rolleprofil:** Hvem observerer, analyserer, beslutter og utfører er en del
   av tvillingens versjonerte definisjon.
2. **Relasjoner:** En beslutning, handling eller resultatregistrering kan
   gjelde flere episoder og prosessobjekter. En enkel episodepeker er et
   forenklet grensesnitt, ikke en begrensning i domenemodellen.
3. **Proveniens:** Kilde, bearbeidingsaktivitet og ansvarlig aktør holdes
   atskilt og kan senere kartlegges til PROV.
4. **Modellgyldighet:** Valideringsstatus knyttes til modellrevisjon, formål,
   datagrunnlag og toleransepolicy, separat fra tjenestens tekniske tilgjengelighet.
5. **Ingen automatisk oppjustering av påstander:** Prosessavvik, modellavvik,
   dårlig datakvalitet og ukjent kontekst har forskjellige begrunnelser.

Dette utvider presisjonen i kontraktene. Det bestiller ingen produksjonskode,
installerer ingen av alternativene og endrer ikke godkjenningsrekkefølgen for OSL.

## 7. Anbefalt avgrensning av første produktleveranse

Det nye [releaseplanforslaget](../roadmap/v1_0_1_plan.md) bygger på følgende
anbefalte første delmengde:

- en versjonert definisjon med mål og rolleprofil;
- ett konsistent historisk snapshot fra allerede autoriserte kilder;
- lesende prosesshistorie med eksisterende episoder og synlig datakvalitet;
- et evidenspanel som viser målte, rapporterte, beregnede og tolkede påstander;
- en relasjonsmodell som ikke låser framtidig operatøroppfølging til én episode;
- syntetiske tester for konsistens, datatilgang og konfidensialitet.

Skriving av menneskehendelser, produksjonsbaseline, anbefalinger og simulering
kan følge som egne leveranser. Det gir et tydelig første resultat uten å gjøre
forbedret kvalitet eller prediksjon til en udokumentert akseptpåstand.

Dette er en anbefaling om omfang. Planen er opprettet som neste trinn etter
beskrivelsen; den er ikke en påstand om at implementasjonen eller hele
utviklerspesifikasjonen allerede er avtalt som én release.

## 8. Forslag til avgrensede undersøkelser før teknologivalg

| Undersøkelse | Utelukkende syntetisk startgrunnlag | Avgjørelse den skal støtte |
| --- | --- | --- |
| Hendelsesrelasjoner | Flere episoder, én beslutning, flere handlinger og et uavklart resultat | Kan vår relasjonskontrakt bevare betydning og kilde uten tvungen én-til-én-kobling? |
| Proveniens | To kilderevisjoner og samme analyse kjørt med ulike policyer | Kan et gammelt resultat fortsatt forklares når nye data eller notater finnes? |
| Modellgyldighet | Uendret prosess med ugyldig modell, og gyldig modell med ukjent kontekst | Skiller produktet årsakene til avståelse uten å finne på en prosessfeil? |
| Ekstern komponent | Dokumentert kapabilitetsgap og separat avhengighets-/lisensvurdering | Er en PM4Py-, AAS-, Ditto- eller simuleringsadapter faktisk nødvendig? |

Ingen av disse forsøkene er kjørt i denne dokumentleveransen. Eventuell privat
pilot følger godkjent datatilgang; eksterne eksempler skal forbli syntetiske.

## 9. Hva forskningen ikke avgjør for oss

Kildene avgjør ikke hvilken faktisk operasjon som er pilot, hvilke kilder som
er tilstrekkelige, hvilke felt som betyr det samme mellom opptak, eller hvilken
terskel som er forsvarlig for en bestemt vurdering. Det må avgjøres gjennom D0
og den faglige sammenligningskontrakten B0 i hovedspesifikasjonen.

De foreslåtte minsteutvalgene i hovedspesifikasjonen er derfor utviklingsforslag,
ikke forskningsvaliderte normalitetsgrenser. Et oppdaget historisk mønster er
heller ikke dokumentasjon av at et tiltak vil forårsake ønsket framtidig resultat.

Før en ny komponent innføres skal beslutningen angi konkret behov, valgt
utgitt versjon, lisensgrunnlag, dataflyt, ansvar for autorisasjon og lagring,
driftskostnad, tilbakeføring og et målbart akseptkriterium. Fram til da beholdes
den eksisterende teknologikjernen.
