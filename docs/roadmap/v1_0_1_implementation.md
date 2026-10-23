# Prosesstvilling: implementasjon, verifikasjon og pilotstatus

Produktkoden for den første historikkleveransen er ferdig implementert og
teknisk verifisert på Windows og Linux. Den kontrollerte referanseprofilen
består. **Privat pilot er ikke gjennomført, og release er ikke publisert.**
Eieren har bekreftet at det nødvendige reelle pilotgrunnlaget ennå ikke finnes.
Den komplette menneskemedierte tvillingen i F01–F14 leveres i senere trinn.

Arbeidsnavnet er v1.0.1. Ingen versjonsfil, tag eller publisert releasehistorikk
er endret. Endelig versjonsvalg følger [planens SemVer-regel](v1_0_1_plan.md#3-versjonsnavn-og-forhold-til-v1).
Dette dokumentet inneholder bare produktkontrakter og syntetiske testresultater.
Lokale produksjonsidentiteter, datoer, filinventarer og innholdsdigester er utelatt.

## Det som er implementert

| Del | Konkret oppførsel | Kode |
| --- | --- | --- |
| Kildebinding | Serverens eksisterende kilderegister bestemmer tilgjengelige input. Nettleseren sender ugjennomsiktige ID-er og forventet binding; ingen frie filstier | `services/process_twin_inputs.py`, `digital_twin/adapters.py` |
| Kanonisk lesing | SQLite åpnes med `mode=ro`, lesetransaksjon og schema-validering. Hele Agent-instansens fingeravtrykk kontrolleres før Device-visning. Ingen segmentering startes | `digital_twin/adapters.py` |
| Legacy | Eksisterende parser, klassifisering og døgnoppsummering gjenbrukes. UTC-antakelse og manglende mål er eksplisitte. Ingen nye episoder lages | `digital_twin/adapters.py`, eksisterende `digital_twin_service.py` |
| Definisjon og revisjoner | Produksjonsprosess, avgrensning, formål, historisk gyldighetsområde og menneske-/systemroller lagres uforanderlig. Endring krever forventet siste revisjon | `services/process_twin_service.py` |
| Snapshot | Eksplisitt bestilling bevarer eksakt datasett, definisjon, originalgrenser, kontekst, evidens, kvalitet og transformasjonsversjoner | `digital_twin/projection.py`, `services/process_twin_service.py` |
| Varigheter | Eksisterende tilstandsintervaller skjæres med halvåpent vindu. Union hindrer dobbeltelling. Ukjent, målt null og ingen dekning har forskjellig betydning | `digital_twin/projection.py` |
| Operatørposter | Autoriserte eksisterende poster og revisjoner fryses. Bare eksplisitte kilderelasjoner er bekreftede; kjent maskin/tid kan gi en ulenket visning. Antatt tid oppgraderes ikke | `digital_twin/adapters.py`, `digital_twin/projection.py` |
| Evidens | Hver vist post har referanse til bevart kilderevisjon og påstandstype. Korrupt/manglende eksakt innhold vises utilgjengelig; dagens tekst brukes ikke som reserve | `services/process_twin_service.py` |
| Lagring | Primærhistorikk og separat visningsindeks. Manifest, kommando, audit og indeks publiseres ved samme SQLite-commitgrense. Gjenoppbygging må ha identisk logisk digest | `digital_twin/records.py` |
| Bestillinger | Aktør-/operasjonsspesifikk idempotens, én tung bygging, lagrede terminalfeil og lease-/fristgjerde. Ingen automatisk ny bygging etter feil | `digital_twin/records.py` |
| Tilgang | Eksisterende innlogging, sentral `data.read`, `workflow.run`/`analysis.run`, faktisk CSRF og aktuelle kilde-/postomfang. Ny kontroll før publisering og ved hver lesing | `process_twin_routes.py`, `auth/policy.py` |
| Web | `/process-twins`: klargjøring, definisjon, historiske snapshots, kvalitet, episoder, operatørposter og evidens. GET starter ingen analyse. Ingen polling, ekstern modell eller CDN på siden | `templates/process_twin.html`, `static/js/process-twin.js` |
| Usikkert HTTP-svar | Nettleseren slår opp opprinnelig bestillingsnøkkel etter brutt forbindelse. Endret ny bestilling blokkeres til utfallet er avklart | `static/js/process-twin.js` |

Kodebanene i tabellen er relative til `catalog/flask_app/`, bortsett fra
`digital_twin/`, som ligger direkte under `catalog/`. API-endepunktene og de
obligatoriske feltene er bestemt i [B03–B13](../specs/production_process_digital_twin_behavior.md).

Eksisterende Recorder-, runtime- og reconnect-kroker hopper over disse
historikkrutene før de berører de respektive tjenestene. Den sentrale
autentiseringen og autorisasjonen kjøres fortsatt. Eksisterende `/digital-twin`
og dens scenarioformel er beholdt. Ingen avhengigheter er installert.

## Lokal oppstart og kildekontrakt

Koden inngår i den eksisterende Flask-appen. Etter vanlig installasjon og
vanlig konfigurering av eksisterende autentisering initialiseres bare DTs eget
lager eksplisitt:

```powershell
python -m flask --app catalog.flask_app.app:create_app process-twin-init
```

Standardlageret er `process_twins/` under appens konfigurerte datarot.
`FCP_PROCESS_TWIN_STORE` i Flask-konfigurasjonen kan overstyre dette. Initialisering
leser ingen kilde, endrer ingen rettighet og aktiverer ingen Recorder.
En vanlig GET initialiserer ikke lageret.

Standardadapteren leser det eksisterende registeret
`source_config/machines_and_sensors.json` under appens datarot. En privat
`process_twin_inputs`-liste kan beskrive allerede eksisterende kilder:

| Felt | Binding |
| --- | --- |
| `input_id` | Stabil `in_` etterfulgt av 32 små heksadesimale tegn; opprettet av server/eier |
| `machine_id` | ID som allerede finnes i registerets `machines` |
| `kind` | `CANONICAL_DEVICE` eller `LEGACY_JSONL` |
| `relative_paths` | Eksisterende tillatte filer innen dataroten; kanonisk input har kanonisk base først og operasjonell base etterpå |
| `binding` for kanonisk input | Eksakt `source_key`, numerisk `agent_instance_id`, eksisterende `device_key` |
| `binding` for legacy | Eksisterende `machine_id`; flere ulike filer krever dokumentert avklart opptaksomfang |
| `clock_binding` | `UTC_NORMALIZED` bare når eierkontrakten faktisk dokumenterer dette; ellers kan `SOURCE_UTC` ikke brukes |
| `origin_status` | `KNOWN` bare ved dokumentert opprinnelse; ellers `UNKNOWN` |

Registeret inneholder identitets-/databeskrivelser, aldri kopierte grants.
Standardadapteren følger den eksisterende lokale `data.read`-rettigheten og
kontrollerer aktuell bruker/rolle gjennom den eksisterende autorisasjonsbasen.
Konfigurerte Federation-mirrorområder godtas ikke som en lokal autoritativ
reservekilde. Manglende register gir utilgjengelighetsfeil; et lesbart tomt
register gir en tom liste.

En eksisterende kildeeier med mer presis post-/datatilgang kobler inn sin
leseadapter gjennom `app.extensions["process_twin_input_registry"]`.
Adapteren oppfyller `InputRegistry.get/list/check_scopes` og returnerer
`InputSource` med en aktuell lokal autorisasjonsfunksjon. Kallet må feile
lukket når autoritet ikke kan kontrolleres. Det skal ikke etablere grants,
kopiere Federation-autoritet eller gjøre nettverkssynkronisering under en DT-lesing.

Operatørinput har `kind=OPERATOR_RECORDS`, eksakt lokal eierrevisjon,
`local_authoritative=True` og postspesifikk autorisasjon. Filadapteren støtter
eksisterende `fcp.operator_strategy_records.v3` og den eksplisitte lokale
leserepresentasjonen `fcp.process_twin.operator_read.v1`. Sistnevnte har
`records` og eksisterende `links`; den oppretter ingen menneskefakta.
Maskinbinding per post og relasjonenes input-/segmentrevisjoner kommer fra
eieren. En generell cachefil eller fritekstens maskinnavn er utilstrekkelig.

Ingen produksjonsregisteroppføringer, pilotbindinger eller operatørdata er
opprettet av implementasjonsarbeidet. Den lokale D0-kontrollen fant ingen
registrert DT-maskinbinding i det undersøkte lokale registergrunnlaget.
Eieren har deretter bekreftet at den nødvendige kombinasjonen av kanonisk
observasjonsbase, tilhørende operasjonell base og ekte operatørregistrering
ikke finnes ennå. Piloten har derfor status `WAITING_FOR_REAL_INPUT`.
Den konkrete [pilotprotokollen](v1_0_1_pilot_acceptance.md) bestemmer innganger,
gjennomføring, evidens og beslutningsregel når slikt materiale foreligger.

## Verifikasjon og begrensninger

Følgende er kontrollert med håndskrevne syntetiske data:

- Python: 175 beståtte funksjons-/integrasjons-/regresjonstester på Windows
  med Python 3.14 og de samme 175 på Linux med Python 3.12. Testene inkluderer
  eksisterende DT, sentral autorisasjon og operasjonell tidslinje, rolleoppløsning
  og projeksjon.
- JavaScript: to beståtte transporttester for tapt svar, samme
  bestillingsnøkkel og blokkering av endret bestilling mens utfallet er ukjent.
- Ekte Flask-app med vanlig innlogging og CSRF, eksisterende kilderegister,
  ingen kildeinitialisering ved GET og tilbakekalling av rettighet før commit.
- Nettleser: innlogging, valg av syntetisk maskin og operatørgrunnlag,
  klargjøring, definisjon, snapshot, ny sideinnlasting av samme historikk,
  episode, ulenket operatørpost og eksakt evidens. Tastaturaktivering og
  HTML-lignende tekst som ren tekst er kontrollert.
- Alle 13 API-leseruter er kontrollert med gyldig lesing, innloggingskrav,
  tilbakekalt kildeomfang, ugyldig query, `no-store` og uendret primærlager.
  Alle fem skriveruter er kontrollert med gyldig request, hvert manglende
  toppnivåfelt, manglende innlogging, tilbakekalt kildeomfang og faktisk
  manglende CSRF uten ekstra reservasjon/skriving. De 18 utvidede rutetestene
  består separat på begge plattformer etter at disse påstandene ble utvidet.
  Nestede obligatoriske definisjons-, rolle- og vindusfelt har egne negative
  kontrakttester med faste, ikke klientstyrte feltstier.
- Ruff, formatering, Python-kompilering, JavaScript-syntaks og diffkontroll
  inngår i avsluttende kildekontroll.

Alle 49 BV-vektorer har eksplisitte testreferanser i
[`acceptance_vectors.json`](../../catalog/digital_twin/tests/acceptance_vectors.json).
Matrisens referanser er kontrollert mot de faktiske beståtte Linux-resultatene.
Den inneholder testselektorer, ingen forhåndsutfylte PASS-påstander. Testnivået
er HTTP, tjeneste eller komponent etter den aktuelle kontrakten; matrisen
hevder ikke at alle mulige kombinasjoner er prøvd over HTTP.

Testene dekker blant annet kildeintegritet, uforanderlighet, Agent-/Device-omfang,
halvåpne vinduer, originale episodegrenser, målte nuller mot ukjente verdier,
posttilgang, samtidig bestilling, idempotens, feilbevaring, sideinndeling,
evidenskorrupsjon uten reservekilde og indeksgjenoppbygging. Historisk
`AVAILABLE` oppgraderer ikke modellstatus fra `NOT_ASSESSED`.

Den faste store syntetiske profilen har 100 000 observasjoner, 10 000 episoder
og 100 menneskeposter. Den kontrollerte Linux-kjøringen brukte fire bundne
logiske CPU-er, 8 GiB minnegrense, lokal verifisert SSD og ingen andre aktive
DT-bygginger. Ti oppvarmingsrequests etterfølges av 100 sekvensielle,
autentiserte HTTP-lesinger med kildeomfangskontroll og `limit=100`.

| Måling | Resultat | Kontrakt |
| --- | --- | --- |
| Klargjøring av revisjon | 12,6 sekunder | Innen uendret operasjonsbudsjett |
| Snapshotbygging | 12,1 sekunder | Under 120 sekunder fra lagret reservasjon |
| HTTP-p95 | 0,006 sekunder | Under 2 sekunder; 95. stigende verdi av 100 |

Tallene gjelder bare den faste syntetiske profilen. Resultat, innholdsdigest,
miljøbinding og rålogger er bevart i privat evidens. De er ikke produksjonsmål.

En tidligere profilkjøring avdekket minneoverskridelse i snapshotbyggingen.
Unødvendige samtidige mellomrepresentasjoner ble fjernet. Den kontrollerte
referansekjøringen avdekket deretter en separat overskridelse ved lagring av
klargjort revisjon: SQLite beholdt store bundne JSON-verdier i statementcachen.
DTs eget lager åpner nå forbindelser med `cached_statements=0`. Ny kontrollert
referansekjøring består med de opprinnelige budsjettene. Begge originale
feil og de tilhørende syntetiske grunnlagene er bevart privat.

Ressurskontrollen er kooperativ. Løkker i DT og de gjenbrukte eierberegningene
har kontrollpunkter; RSS prøves ved første kontrollpunkt etter 100 ms siden
forrige prøve, eller ved en tvungen kontroll. Blokkerende bibliotek-/lagringskall kontrolleres
ved grensene. Fristen bindes til bestillingens faktisk lagrede monotone
reservasjonstid. Commit-gjerdet hindrer publisering etter frist, leasebrudd
eller registrert minneoverskridelse, også når et blokkerende kall kommer sent
tilbake. Dette er ikke en garanti om prosessavslutning midt inne i et slikt kall.
De eksisterende eierfunksjonenes nye kontrollcallback er valgfri og endrer
ikke deres klassifikasjon eller standardresultater.

Det opprinnelige Linux-importforsøket med for gammel Python er bevart.
Endelig Linux-kvalifisering brukte eksisterende Python 3.12-runtime og allerede
tilgjengelige testverktøy montert skrivebeskyttet. Ingen pakker ble installert.
Testmiljøet hadde ingen nettverkstilgang; kildekoden var skrivebeskyttet og
testlagringen separat fra appens produksjonslagring.

## Portstatus og overlevering

| Port | Status | Nødvendig konkret arbeid |
| --- | --- | --- |
| Lokal programvare og syntetisk BV-/API-kontrakt | PASS | 49 sporbare BV-vektorer; 13 lese-/5 skriveruter og relevante komponent-/integrasjonstester |
| Windows og Linux | PASS | 175 funksjons-/integrasjons-/regresjonstester på hver plattform; utvidede 18 rutetester består på begge |
| B15 referanseprofil | PASS | Kontrollert 4-CPU/8-GiB/SSD-kjøring; uendrede grenser og bevart resultat |
| JavaScript og syntetisk brukerforløp | PASS | To transporttester og gjennomført nettleserforløp |
| Privat D0-kildebinding | WAITING_FOR_REAL_INPUT | Nødvendig reelt pilotgrunnlag finnes ennå ikke; bruk faktisk kildeeier når det foreligger |
| Autoritativ operatørlesing i pilot | WAITING_FOR_REAL_INPUT | Minst én ekte menneskepost med legitim lokal eieradapter og aktuell posttilgang |
| AP-01 | Ikke gjennomført | Følg [pilotprotokollen](v1_0_1_pilot_acceptance.md), med ekte faglig vurdering og bevart avviksliste |
| Release | Ikke publisert | Pilotaksept, endelig kandidatgjennomgang og versjonsvalg før eventuell tag/publisering |

Programvareleveransen er klar for gjennomgang. Primærmanifest, audit, opprinnelige
feil og privat verifikasjon er bevart. Ingen produksjonskilde eller Recorder er
aktivert gjennom dette arbeidet. AP-01 kan ikke erstattes med syntetiske data
eller gamle V1-bevis. Ingen gammel V1-automatisering er startet på nytt.
