# Privat aksept av historikkgrunnlaget for prosesstvillingen

Denne protokollen utfører AP-01 i
[kravspesifikasjonen](../specs/production_process_digital_twin_requirements.md#122-ap-01-privat-historikkpilot)
og B15.1 i [atferdskontrakten](../specs/production_process_digital_twin_behavior.md#b151-ytelsesoppskrift-og-pilot).
Den beskriver en senere kjøring. **Ingen reell pilot er gjennomført.** Eieren
har bekreftet at nødvendig pilotgrunnlag ikke finnes ennå. Status er
`WAITING_FOR_REAL_INPUT`; ingen akseptdato eller menneskelig godkjenning er satt.

Protokollen krever ingen nye opptak, kildeaktivering eller produksjonsendring
for å kunne markere et resultat. Manglende grunnlag beholder ventestatus.
Alle faktiske identiteter, produksjonstider, stier, tellinger, revisjoner,
digester, skjermbilder og mennesketekster oppbevares tilgangsstyrt lokalt.
Dette dokumentet skal bare inneholde generelle instrukser og feltdefinisjoner.

## 1. Inngangsport

Følgende må være sant før gjennomføring. Hvert punkt får et faktisk privat
bevis eller status `MISSING`; antakelser teller ikke som bevis.

| Inngang | Bindende kontroll |
| --- | --- |
| Programvarekandidat | Registrer faktisk Git-HEAD, base-tree og manifest med digest for alle inkluderte endrede og nye filer. Lokal dirty diff skal inngå. Et uendret HEAD alene identifiserer ikke lokal kode |
| Maskingrunnlag | Én eksisterende kanonisk observasjonsbase og dens tilhørende operasjonelle base kan leses gjennom deres eierkontrakter |
| Konsistens | Eksakt source-key, Agent-instans, Device-identitet og projeksjon/fingeravtrykk er konsistente. En projeksjon for en annen Agent-revisjon godtas ikke |
| Menneskegrunnlag | Minst én faktisk operatør-/fagregistrering finnes med opprinnelig ID, eierrevisjon, maskinbinding og faktisk posttilgang. En syntetisk post eller en ny DT-tekst godtas ikke |
| Tilgang | Eksisterende innlogging og lokal kilde-/postautorisasjon kan kontrolleres. Ikke opprett eller kopier grants for å få et testutfall |
| Rollefordeling | Navngi virkelig utførende person og operatør/fagperson i privat protokoll. Automatiske testresultater signeres ikke som menneskelig vurdering |
| Utvalg | Bind konkret historikkvindu og oppgaveliste før resultatet undersøkes. Inkluder normale, delvise og tvetydige forløp når de finnes; registrer fravær |
| Eget lager | DTs eget lager er eksplisitt initialisert i godkjent appmiljø. Kildene skal ikke initialiseres, migreres eller repareres av DT |

Hvis én nødvendig inngang mangler, avsluttes piloten med `WAITING_FOR_REAL_INPUT`
eller `BLOCKED` med konkret årsak. Ikke erstatt kanoniske episoder med
legacy-aggregater. Ikke endre input etter å ha sett et uønsket resultat uten
å bevare det opprinnelige forsøket og registrere et nytt, separat utvalg.

## 2. Koble eksisterende materiale

Kildeeieren binder allerede eksisterende materiale gjennom
[kildekontrakten](v1_0_1_implementation.md#lokal-oppstart-og-kildekontrakt).
Nettleseren får bare serverens input-ID og bindingsrevisjon. Frie stier eller
produksjonstekst skal ikke legges i URL-er, repoet eller en ekstern tjeneste.

Standardregisterets `process_twin_inputs` gjelder maskininput. Operatørmateriale
krever en legitim lokal eieradapter gjennom
`app.extensions["process_twin_input_registry"]`, med `OPERATOR_RECORDS`, eksakt
eierrevisjon og aktuell postspesifikk autorisasjon. Adapteren leser eksisterende
registreringer og relasjoner; den lager ikke en bekreftet relasjon fra navnelikhet.
Registeret er en databeskrivelse, ingen autoritetskilde for nye rettigheter.

Bevar privat hvilke opprinnelige kilderevisjoner som brukes. Kontroller med
kildeeierens lesemetode at DT ikke har endret kildeinnhold eller schema.
En eventuell ny kildeaktivitet utenfor DT må registreres særskilt; en endret
aktiv kilde kan ikke feilaktig merkes uendret ut fra et senere filhash alene.

## 3. Gjennomfør det eksplisitte brukerforløpet

Bruk vanlig innlogging og åpne `/process-twins` i det godkjente miljøet.
Bare materialet brukeren allerede har tilgang til skal være synlig.

1. Åpne «Klargjør eksisterende datagrunnlag». Velg avtalt maskingrunnlag og
   ekte operatørgrunnlag. Velg tidsregel uttrykkelig. «Dokumentert UTC» krever
   faktisk dokumentasjon; innsamlingstid beviser ikke hendelsestid. Klikk
   «Klargjør revisjon» én gang. Bevar returnert datasett-/bindingsrevisjon privat.
2. Åpne «Definer produksjonsprosessen». Beskriv faktisk motpart, prosessgrense
   og hva historikken kan brukes til. Kontroller den lagrede definisjonens mål,
   egenskaper, rolleprofil og gyldighetsområde mot avtalt historikkomfang.
   Registrer definisjonen fra den klargjorte revisjonen og bevar revisjons-ID-en.
3. Velg prosessen. Åpne «Opprett historisk snapshot». Angi det forhåndsvalgte
   halvåpne UTC-vinduet med seks mikrosekundsiffer. Start inngår, slutt inngår
   ikke. Vinduet skal være innen kontraktens 31 dager. Klikk «Bygg valgt historikk»
   én gang. Bevar kommandonøkkel, definisjonsrevisjon og snapshot-ID privat.
4. Hvis HTTP-svaret går tapt, bruk «Kontroller bestillingsstatus». Avklar den
   opprinnelige nøkkelen før en annen bestilling. Ikke anta at manglende svar
   betyr at ingenting er skrevet, og ikke send samme bestilling med ny nøkkel.
5. Gå gjennom historikk, kvalitet, episoder, operatørposter og detaljer/evidens
   med oppgavelisten under. En mislykket bestilling og dens opprinnelige feil
   beholdes. Nytt forsøk krever et faktisk avklart grunnlag; ingen blind retry.
6. Last siden på nytt, velg det samme lagrede snapshotet og gjenta lesingen.
   Dette skal lese frosset historikk og skal ikke klargjøre eller bygge på nytt.

Ingen GET skal starte Recorder, segmentering, scenario, publisering eller
bakgrunnsbygging. En ny definisjon eller et nytt datagrunnlag gir ingen rett
til å omskrive et eldre snapshot.

## 4. Faglig oppgaveliste og forventet oppførsel

Hvert punkt får `PASS`, `FAIL` eller `NOT_PRESENT` med privat evidensreferanse.
`NOT_PRESENT` brukes bare der det forhåndsvalgte materialet faktisk mangler
et fenomen, ikke som erstatning for en feil eller for obligatorisk menneskepost.

| Kontroll | Forventet resultat |
| --- | --- |
| Motpart og grense | Fagpersonen gjenkjenner det valgte maskinforløpet og kan forklare hva som ligger innenfor og utenfor definisjonen |
| Påstandstype | Observasjon, menneskelig rapport og avledning kan skilles. En operatørforklaring vises ikke som målt maskinfaktum eller bevist årsak |
| Kildebinding | Hver generert faktapåstand/slutning har eksakt kilde-/metodereferanse eller eksplisitt avståelse. Ingen stilltiende kobling til andre Devices eller revisjoner |
| Original episode | Episodens originale ID, klassifikasjon og grenser kan spores. Visning i valgt vindu bevarer originalgrensene og viser eventuell avskjæring |
| Tidsregning | Halvåpne intervaller og unionsregning gir ingen dobbeltelling. Målt null, manglende dekning og uavklart tid er forskjellige resultater |
| Brudd og mangler | Foreliggende sekvensbrudd, ukjent tid, ufullstendig kontekst og manglende grunnlag er synlige. Finnes ikke fenomenet, registreres dette privat |
| Menneskeregistrering | Minst én ekte post kan åpnes med sin opprinnelige eierrevisjon. Relasjon vises bare som bekreftet hvis en eksisterende eksplisitt relasjon beviser den; ellers er den uavklart/ulenket |
| Eksakt evidens | Åpnet evidens svarer til bevart revisjon. Utilgjengelig eller korrupt innhold gir forklart utilgjengelighet, aldri dagens tekst som reserve |
| Samme snapshot | Etter ny sideinnlasting er snapshot-/definisjons-/innholdsrevisjon og historisk betydning identisk; ingen ny analyse er startet |
| Tjenestestatus | Tilgjengelig historie presenteres ikke som godkjent modell, prediksjon, årsakseffekt eller full tilbakemeldingssløyfe |
| Kildevern | Kildeeierens kontroll viser at DTs handlinger ikke endret opprinnelig materiale, schema eller tilgang |
| Konfidensialitet | Faktiske pilotverdier forblir på godkjente private flater. Allmenne logger, URL-er, testfixtures og delbar dokumentasjon inneholder ikke produksjonsinnhold |

Tilbakekalling av rettighet og feilinjeksjon er allerede dekket syntetisk.
Piloten skal ikke endre produksjonsrettigheter, korrumpere primærhistorikk
eller slette en indeks for å gjenskape disse kontrollene. Hvis tilgang endres
naturlig under piloten, registreres faktisk utfall mot kontrakten.

## 5. Privat evidens og akseptbeslutning

Bevar en separat tilgangsstyrt protokoll med følgende obligatoriske felt:

| Feltgruppe | Innhold |
| --- | --- |
| Kandidat | Faktisk HEAD/base-tree, filmanifest og dirty-diff-digest; referanser til tekniske testresultater og deres kvalifiserte kildeversjon |
| Datagrense | Autorisert lokal eier-/inputreferanse, bindingsrevisjon, kanonisk/operasjonell revisjon, datasett-ID og valgt tidsregel |
| Prosess | Definisjons-ID/-revisjon, valgt forhåndsvindu og begrunnelse for utvalg |
| Kommando og snapshot | Opprinnelig kommandonøkkel, lagret utfall, snapshot-ID og innholdsrevisjon |
| Menneskegrunnlag | Eksakt post-/eierrevisjon og faktisk relasjonsstatus, med nødvendig tilgangskontroll |
| Oppgaver | Hvert punkt i avsnitt 4: forventet/faktisk resultat, status og referanse til privat evidens |
| Avvik | Original feil, berørt krav, alvorlighet og reproduserbart privat grunnlag. Et eventuelt senere forsøk refererer til det opprinnelige |
| Faglig beslutning | Virkelig fagpersons identitet, faktisk vurderingstid, vurdering, gyldighetsgrense og beslutning. Ingen automatisk utfylt signatur eller dato |

AP-01 er `PASS` bare når inngangsporten er oppfylt, alle obligatoriske oppgaver
består, sporbarheten er fullstendig og en virkelig operatør/fagperson har
vurdert resultatet. Ukjent relasjon kan være korrekt produktoppførsel, men
skal ikke omtales som demonstrert samlet prosesskjede. Fravær av ikke
obligatoriske fenomener begrenser pilotens dokumenterte dekning.

Kritisk feil i tilgang, kildevern, identitet eller snapshotkonsistens gir `FAIL`
og blokkerer aksept. Andre avvik får eksplisitt kravstatus. Et uavklart krav
markeres ikke bestått. AP-01 dokumenterer historikkaksept innen valgt omfang;
produksjonseffekt, modellgyldighet og senere funksjoner krever egne bevis.

Delbar oppsummering skal bare angi gjennomført funksjonsomfang, overordnet
akseptstatus og gjenstående produktbegrensninger. Faktiske private feltverdier
skal ikke kopieres til repoet, en offentlig PR eller en ekstern modell.
