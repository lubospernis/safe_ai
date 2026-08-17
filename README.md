# SAFE AI

Automatizovaná analytická pipeline pre ECB SAFE reporty o podmienkach financovania a podnikateľskom prostredí na Slovensku.

README je určený používateľovi, ktorý repozitár otvorí na GitHube a potrebuje:

- pochopiť, čo projekt robí;
- pripraviť lokálne prostredie;
- nastaviť prístupy k databáze a modelom;
- spustiť hlavný alebo adhoc report;
- spustiť testy a dbt transformácie;
- pochopiť, čo sa publikuje automaticky cez GitHub Actions;
- upraviť reportové sekcie, SQL alebo konfiguráciu.

> Projekt je v súčasnosti navrhnutý pre MotherDuck. Lokálny offline režim s lokálnou DuckDB databázou nie je podporovaný.

## Obsah

- [Čo projekt robí](#čo-projekt-robí)
- [Dátový tok](#dátový-tok)
- [Štruktúra repozitára](#štruktúra-repozitára)
- [Požiadavky](#požiadavky)
- [Inštalácia](#inštalácia)
- [Konfigurácia](#konfigurácia)
- [Spustenie pipeline](#spustenie-pipeline)
- [dbt transformácie](#dbt-transformácie)
- [Testy a validácia](#testy-a-validácia)
- [GitHub Actions a publikovanie](#github-actions-a-publikovanie)
- [Úprava reportu](#úprava-reportu)
- [Bezpečnosť a tajomstvá](#bezpečnosť-a-tajomstvá)
- [Riešenie problémov](#riešenie-problémov)

## Čo projekt robí

Pipeline premieňa verejné údaje ECB SAFE na opakovateľný report:

1. `dlt` načíta ECB SAFE microdata a uloží ich do MotherDuck.
2. `dbt` vytvorí analytické mart-y s váženými ukazovateľmi.
3. Preddefinované SQL dotazy pripravia podklady pre reportové sekcie.
4. Anthropic a Mistral vytvoria zistenia, executive summary, implikácie a slovenský preklad.
5. Grounding, kvalitatívne, štylistické a grafické kontroly overia výstup.
6. Pipeline vytvorí HTML a JSON artefakty pre statický report a webový portál.

Report obsahuje napríklad financovaciu medzeru, dostupnosť bankových úverov, úverové podmienky, podnikateľské problémy, očakávania a vývoj podnikania. Adhoc pipeline automaticky rozpozná špeciálny modul v novej vlne a vytvorí samostatný spotlight, ak sú také údaje dostupné.

AI nemá voľný prístup k databáze. Agentické dohľadanie údajov používa nástroj `query_mart`, ktorý povoľuje iba schválené read-only `SELECT` dotazy nad whitelisted tabuľkami.

## Dátový tok

```text
ECB SAFE ZIP / annex
        |
        v
 dlt ingestion
        |
        v
MotherDuck: raw + main_safe
        |
        v
 dbt models and marts
        |
        v
reports/sql/*.sql
        |
        v
AI synthesis, translation, quality checks
        |
        +--> reports/output/*.html
        +--> reports/output/*.json
        +--> GitHub Pages / web portal
```

Dôležitá hranica: lokálne spustenie vytvorí súbory v `reports/output/`, ale samo osebe ich necommitne, nepublikuje na GitHub Pages a neodošle newsletter. Publikovanie zabezpečujú GitHub Actions workflow.

## Štruktúra repozitára

| Cesta | Účel |
| --- | --- |
| `pipeline.py` | Vstupný bod pre načítanie ECB SAFE microdata cez dlt. |
| `safe_microdata/` | dlt zdroje pre SAFE microdata a annex. |
| `dbt_project/` | dbt projekt, staging, intermediate a mart modely. |
| `reports/run_report.py` | Hlavná reportová pipeline. |
| `reports/run_adhoc_report.py` | Pipeline pre adhoc spotlight. |
| `reports/sql/` | Preddefinované SQL dotazy reportových sekcií. |
| `reports/config.py` | Registrácia sekcií, názvy dotazov, signové pravidlá a grafy. |
| `reports/db.py` | MotherDuck pripojenie, načítanie dát a validácia `query_mart`. |
| `reports/llm.py` | Modelové volania, structured output, grounding a agentické spracovanie. |
| `reports/quality_check.py` | Kvalitatívna brána a kontrola grafov. |
| `reports/output/` | Lokálne vytvorené reporty, JSON payloady, logy a skóre kvality. |
| `validation/` | Porovnanie výstupov s publikovanými ECB sériami. |
| `tests/` | Unit a integračné testy s mockovanými externými službami. |
| `web/` | Next.js portál, autentifikácia, grafy a spätná väzba. |
| `.github/workflows/` | Automatizácia ingestu, dbt, reportov, validácie a publikovania. |

## Požiadavky

- Python 3.11.
- Git.
- MotherDuck účet a service token.
- Anthropic API key pre hlavný report.
- Mistral API key pre implikácie, redakciu, preklad a kontroly.
- Prístup k internetu pre ECB, MotherDuck a modelové API.
- Pre frontendovú časť aj Node.js a npm.
- Pre dbt samostatné závislosti z `dbt_project/requirements.txt`.

Pipeline používa MotherDuck databázu `my_db` a produkčnú schému `main_safe`. Predpokladá sa, že dbt modely a zdrojové tabuľky boli vytvorené v tejto databáze.

## Inštalácia

### Windows PowerShell

```powershell
git clone <URL_REPOZITÁRA>
cd safe_ai

py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Ak PowerShell zablokuje aktiváciu prostredia, aktivujte ju iba pre aktuálne okno:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

### Linux alebo macOS

```bash
git clone <URL_REPOZITÁRA>
cd safe_ai
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### dbt balíky

Ak chcete spúšťať dbt transformácie, nainštalujte závislosti definované pre dbt projekt:

```powershell
cd dbt_project
python -m pip install -r requirements.txt
dbt deps --profiles-dir . --project-dir .
cd ..
```

Na Linuxe a macOS použite rovnaké príkazy s aktivovaným virtuálnym prostredím.

## Konfigurácia

### Premenné prostredia

V koreňovom adresári vytvorte `.env`. Súbor je gitignored a nesmie sa commitovať:

```dotenv
MOTHERDUCK_TOKEN=<motherduck-service-token>
ANTHROPIC_API_KEY=<anthropic-api-key>
MISTRAL_API_KEY=<mistral-api-key>
```

`MOTHERDUCK_TOKEN` je potrebný pre databázové dotazy, cache a väčšinu pipeline. `ANTHROPIC_API_KEY` a `MISTRAL_API_KEY` sú potrebné pri generovaní reportu. Testy externé volania mockujú a tieto kľúče nepotrebujú.

### dlt konfigurácia

Súbor `.dlt/config.toml` definuje verejný zdroj ECB:

```toml
[sources.safe_microdata_source]
zip_url = "https://www.ecb.europa.eu/stats/ecb_surveys/safe/shared/pdf/ecb.SAFE_microdata.zip"
fallback_scrape_url = "https://www.ecb.europa.eu/stats/ecb_surveys/safe/html/data.en.html"
request_timeout = 60

[load]
workers = 3
```

Ak ECB zmení primárnu URL, upravte `zip_url`. `fallback_scrape_url` slúži na nájdenie nového ZIP odkazu na stránke ECB.

### MotherDuck a dbt

`dbt_project/profiles.yml` používa:

```yaml
safe_ai:
  target: prod
  outputs:
    prod:
      type: duckdb
      path: "md:my_db"
      extensions:
        - motherduck
```

CI odovzdáva token dbt-u ako premennú `motherduck_token`. Pri manuálnom dbt behu nastavte rovnakú premennú v shelli alebo použite konfiguráciu podporovanú dlt/MotherDuck klientom.

### Webová aplikácia

Webová časť v `web/` má vlastnú konfiguráciu Next.js a Supabase. Je oddelená od Python reportovej pipeline. Pred lokálnym spustením webu nastavte premenné požadované súbormi v `web/lib/` a konfiguráciou Supabase. Webový portál načítava hotové JSON payloady reportu; nevykonáva hlavné analytické dotazy priamo do MotherDuck.

## Spustenie pipeline

Pred každým behom aktivujte virtuálne prostredie a načítajte `.env` do procesu. V PowerShelli použite:

```powershell
.\.venv\Scripts\Activate.ps1
$env:MOTHERDUCK_TOKEN = (Get-Content .env | Where-Object { $_ -match '^MOTHERDUCK_TOKEN=' } | ForEach-Object { $_ -replace '^MOTHERDUCK_TOKEN=', '' })
$env:ANTHROPIC_API_KEY = (Get-Content .env | Where-Object { $_ -match '^ANTHROPIC_API_KEY=' } | ForEach-Object { $_ -replace '^ANTHROPIC_API_KEY=', '' })
$env:MISTRAL_API_KEY = (Get-Content .env | Where-Object { $_ -match '^MISTRAL_API_KEY=' } | ForEach-Object { $_ -replace '^MISTRAL_API_KEY=', '' })
```

Python skripty používajú `python-dotenv` a načítajú `.env` pri spustení; pri dbt alebo subprocessoch je však potrebné zabezpečiť, aby boli premenné aj exportované do procesu.

### Hlavný report

```powershell
python reports/run_report.py
```

Výstupom je najnovšia dostupná vlna v angličtine a slovenčine, vrátane HTML a JSON súborov v `reports/output/`.

### Report pre historickú vlnu

```powershell
python reports/run_report.py --wave 37
```

`--wave N` obmedzí dáta na vlnu `N`. Používa sa na retrospektívne reporty a bezpečné overenie historickej vlny bez zmeny databázy.

### Obnovenie cache

```powershell
python reports/run_report.py --no-cache
```

Použite, keď chcete znova vykonať všetky modelové fázy. Bežný beh využíva lokálnu cache aj cache v MotherDuck, aby znížil náklady a počet API volaní.

### Opakovanie vybraných sekcií

```powershell
python reports/run_report.py --rerun-sections financing_gap,bank_loan_terms
```

Týmto sa vynútia vybrané sekcie. Následné fázy sa podľa hashov vstupov znovu vyhodnotia automaticky.

### Adhoc report

```powershell
python reports/run_adhoc_report.py
python reports/run_adhoc_report.py --wave 37
```

Ak aktuálna vlna nemá adhoc modul, skript skončí bez reportu. Ak modul existuje, vytvorí anglický a slovenský adhoc spotlight.

## dbt transformácie

dbt transformácie sa štandardne spúšťajú po úspešnom načítaní microdata:

```powershell
cd dbt_project
dbt deps --profiles-dir . --project-dir .
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .
cd ..
```

Pri novej vlne je odporúčaný postup:

1. načítať microdata cez dlt;
2. spustiť `dbt run --select mart_safe__adhoc_responses`, ak ide o adhoc modul;
3. aktualizovať annex, ak sa zmenili otázky alebo odpovede;
4. overiť novú vlnu spustením `reports/run_adhoc_report.py` alebo adhoc workflow podľa dostupnej konfigurácie;
5. spustiť hlavný report.

Nový neznámy adhoc modul zvyčajne nevyžaduje zmenu Python kódu. Generická pipeline ho môže rozpoznať z mart-u; manuálny fallback názov sa pridáva iba vtedy, ak automatická klasifikácia nie je dostatočná.

## Testy a validácia

### Python testy

```powershell
.\.venv\Scripts\python.exe -m pytest tests/ -q
```

Testy mockujú Anthropic, Mistral a MotherDuck. Pri testoch preto nemá dochádzať k reálnym plateným API volaniam.

### Validácia údajov

```powershell
.\.venv\Scripts\python.exe validation/validate.py
```

Validácia porovnáva vybrané mart ukazovatele s publikovanými ECB sériami a používa toleranciu pre zaokrúhlenie. Výsledok slúži ako dátová kontrola, nie ako náhrada kvalitatívnej brány.

### Kvalitatívna brána

```powershell
.\.venv\Scripts\python.exe reports/quality_check.py --html reports/output/report_latest.html
.\.venv\Scripts\python.exe reports/quality_check.py --html reports/output/report_latest_sk.html
```

Hlavný report používa prah 7/10 a adhoc report prah 8/10. Tier-2 zlyhanie má blokovať publikovanie.

## GitHub Actions a publikovanie

Workflow sú uložené v `.github/workflows/`:

| Workflow | Účel |
| --- | --- |
| `safe_microdata.yml` | Načítanie ECB SAFE microdata cez dlt. |
| `dbt_transform.yml` | Spustenie dbt modelov a dbt testov. |
| `generate_report.yml` | Generovanie, kontrola a publikovanie hlavného reportu. |
| `generate_adhoc_report.yml` | Generovanie a publikovanie adhoc reportu. |
| `generate_report_manual.yml` | Manuálny retrospektívny report, určený na overenie bez produkčného publishu. |
| `validate.yml` | Dátová validácia. |
| `eval_harness.yml` | Evaluačné testy kvality modelových výstupov. |

Hlavný automatický tok je:

```text
safe_microdata.yml -> dbt_transform.yml -> generate_report.yml
```

V repozitári GitHub nastavte v **Settings → Secrets and variables → Actions** minimálne:

- `MOTHERDUCK_TOKEN`
- `ANTHROPIC_API_KEY`
- `MISTRAL_API_KEY`

Pri reportovom workflow GitHub Actions:

1. nainštaluje Python závislosti;
2. spustí report;
3. uloží výstup a quality scores ako artefakty;
4. vyhodnotí publikačnú bránu;
5. commitne run log;
6. pri úspešnej bráne premenuje slovenský report na `index.html` a anglický na `en.html`;
7. publikuje `reports/output/` na branch `gh-pages`.

### Manuálne spustenie workflow

```bash
gh workflow run generate_report.yml
gh workflow run generate_adhoc_report.yml
gh workflow run generate_report_manual.yml
gh run list --limit 10
gh run view <RUN_ID> --log-failed
```

Workflow s príponou `_manual.yml` používajte na retrospektívne alebo testovacie spustenia. Workflow bez tejto prípony môže commitovať zmeny a publikovať živý report.

## Úprava reportu

### Pridanie alebo zmena reportovej sekcie

1. Pridajte alebo upravte SQL súbor v `reports/sql/`.
2. Zaregistrujte sekciu v `reports/config.py`.
3. Nastavte najmä:
   - `id` a `title`;
   - `sql_file`;
   - `question_ids`;
   - `sign_note`;
   - `value_col`, `panel_col` a `series_col`;
   - `focus`, `routed` a prípadné `sme_sql_file`.
4. Overte názvy stĺpcov oproti schéme mart-u.
5. Spustite testy a report s `--wave N`.
6. Skontrolujte grounding, znamienka, graf a quality scores.

### Zmena textu alebo pravidiel AI

Promptové pravidlá sú v `reports/llm.py`, prípadne v orchestrace `reports/run_report.py`. Pri úprave promptu overte:

- že model dostáva zdrojové dáta a signové pravidlá;
- že výstup má predpísanú JSON štruktúru;
- že sa čísla dajú overiť voči podkladom;
- že sa neznížila úroveň quality gate;
- že testy pre grounding a štýl stále prechádzajú.

### Zmena grafu

Konfigurácia panelov a typov grafov je v `reports/config.py`; samotná vizualizácia je v `reports/charts.py`. Pri zmene grafu overte prázdne dáta, popisy osí, legendu, slovenské štítky a zobrazenie na mobilnom portáli.

## Bezpečnosť a tajomstvá

- Nikdy necommitujte `.env`, `.dlt/secrets.toml`, API keys ani databázové tokeny.
- Používajte GitHub Actions Secrets, nie hodnoty priamo vo workflow súboroch.
- Ak sa token objaví v commite, logu alebo zdieľanom artefakte, okamžite ho zneplatnite, vytvorte nový a skontrolujte históriu repozitára.
- Dátová pipeline posiela textové alebo štruktúrované podklady do externých modelových API. Pred pridaním interných alebo dôverných údajov treba schváliť klasifikáciu dát, právny základ, retenčné pravidlá a modelovú architektúru.
- `query_mart` musí zostať read-only a obmedzený na whitelisted tabuľky.

## Riešenie problémov

### `MOTHERDUCK_TOKEN` nie je nastavený

Skontrolujte, že `.env` existuje, obsahuje správny názov premennej a že premenná je dostupná v procese:

```powershell
Test-Path .env
$env:MOTHERDUCK_TOKEN
```

Po zmene `.env` reštartujte shell alebo premenné načítajte znova. MotherDuck je povinný; lokálny DuckDB fallback projekt nepodporuje.

### Modelové API zlyháva

- Skontrolujte `ANTHROPIC_API_KEY` a `MISTRAL_API_KEY`.
- Overte dostupný kredit, rate limit a stav príslušnej služby.
- Skúste najprv historický alebo menší beh s `--wave N`.
- Bežný pipeline používa retry a cache; pri opakovanom zlyhaní skontrolujte run log a príslušný GitHub Actions artefakt.

### dbt nenájde profil alebo MotherDuck

Spúšťajte dbt z adresára `dbt_project` a vždy uveďte:

```powershell
dbt run --profiles-dir . --project-dir .
```

Overte aj token odovzdaný ako `motherduck_token` a kompatibilnú verziu DuckDB/MotherDuck extension.

### Report sa vytvoril lokálne, ale nie je online

Lokálny `run_report.py` iba zapíše súbory do `reports/output/`. Skontrolujte GitHub Actions workflow, quality scores a stav publikačnej brány. Samotný lokálny beh nepushne branch `gh-pages`.

### Adhoc report nevznikol

Skript skončí úspešne aj vtedy, keď v najnovšej vlne nie sú adhoc údaje. Skontrolujte `mart_safe__adhoc_responses`, spustite príslušný dbt model a overte detekciu modulu.

## Príklady výstupov

Po úspešnom behu sa v `reports/output/` typicky nachádzajú:

```text
report_latest.html
report_latest_sk.html
report_payload_latest.json
report_payload_latest_sk.json
run_log.json
quality_scores.json
quality_scores_sk.json
```

Tieto súbory sú generované artefakty. Neupravujte ich ručne ako zdroj pravdy; zmenu vykonajte v SQL, konfigurácii alebo pipeline a report vygenerujte znova.

## Licencia a použitie

Pred použitím projektu s internými alebo dôvernými údajmi NBS treba schváliť bezpečnostné, licenčné, dátové a prevádzkové podmienky. Verejné ECB údaje, modelové API, MotherDuck a hosting majú vlastné podmienky používania a limity.
