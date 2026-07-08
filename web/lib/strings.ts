export type Lang = "en" | "sk";

export const STRINGS = {
  en: {
    subscriptionService: "Newsletter subscription service",
    signOut: "Sign out",
    subscribe: "Subscribe",
    unsubscribe: "Unsubscribe",
    somethingWentWrong: "Something went wrong",
    formatDescription: "Sent by email — subscribe to each newsletter independently below.",
    lastUpdated: "Updated {date}",
    nextRelease: "Next release: {date}*",
    nextReleaseFootnote: "*Newsletter will be sent out around this date.",
    newsletters: {
      "safe-regular": {
        name: "SAFE Slovakia",
        description:
          "Quarterly ECB Survey on the Access to Finance of Enterprises — Slovakia focus. Covers financing conditions, loan applications, business situation, and forward-looking expectations.",
        periodicity: "Quarterly",
      },
      "safe-adhoc": {
        name: "SAFE Slovakia — Special Focus",
        description:
          "Ad-hoc deep dive on a special survey topic (e.g. AI adoption, green transition), sent whenever the ECB adds a one-off module to the SAFE survey.",
        periodicity: "Ad hoc",
      },
    },
    chat: {
      title: "Ask SAFE Data",
      inputPlaceholder: "Ask a question about SAFE survey data…",
      send: "Ask",
      loading: "Thinking…",
      costLabel: "Cost: ${cost}",
      errorGeneric: "Something went wrong answering that question.",
      errorRateLimit: "You've reached the question limit for now — try again later.",
      noTableData: "No table data for this answer.",
      navLink: "Ask SAFE Data",
      introDescription:
        "Ask a question in plain English about the ECB SAFE survey — financing conditions, business situation, loan applications, expectations, and more. The answer is generated from a live query against the underlying data, with the SQL and cost shown for transparency.",
      introDimensionsTitle: "Try comparing across:",
      introDimensionCountry: "Countries — Slovakia vs the euro area, or vs a specific peer like Germany",
      introDimensionFirmSize: "Firm size — all firms vs SMEs only vs large firms",
      introDimensionTime: "Time — trend over the last several waves, not just the latest one",
      introTrendingTitle: "Trending now in the latest wave",
      introTrendingWorsened: "worsened by {delta}pp since the prior wave",
      introTrendingImproved: "improved by {delta}pp since the prior wave",
      introExamplesTitle: "Example questions",
      introExampleCountry: "How does Slovakia's bank loan rejection rate compare to the euro area?",
      introExampleFirmSize: "Is the financing gap different for SMEs than for large firms in Slovakia?",
      introExampleTrend: "How has Slovakia's investment outlook changed over the last 4 waves?",
      showSql: "Show SQL",
    },
  },
  sk: {
    subscriptionService: "Služba odberu newslettra",
    signOut: "Odhlásiť sa",
    subscribe: "Odoberať",
    unsubscribe: "Zrušiť odber",
    somethingWentWrong: "Niečo sa pokazilo",
    formatDescription: "Zasielané e-mailom — každý newsletter si môžete odoberať samostatne.",
    lastUpdated: "Aktualizované {date}",
    nextRelease: "Ďalšie vydanie: {date}*",
    nextReleaseFootnote: "*Newsletter bude odoslaný približne v tomto termíne.",
    newsletters: {
      "safe-regular": {
        name: "SAFE Slovensko",
        description:
          "Štvrťročný prieskum ECB o prístupe firiem k financovaniu — zameranie na Slovensko. Zahŕňa podmienky financovania, žiadosti o úvery, obchodnú situáciu a výhľad do budúcnosti.",
        periodicity: "Štvrťročne",
      },
      "safe-adhoc": {
        name: "SAFE Slovensko — Špeciálna téma",
        description:
          "Mimoriadny prehľad na špeciálnu tému prieskumu (napr. adopcia AI, zelená transformácia), zasielaný vždy, keď ECB doplní do prieskumu SAFE jednorazový modul.",
        periodicity: "Príležitostne",
      },
    },
    chat: {
      title: "Opýtať sa na dáta SAFE",
      inputPlaceholder: "Položte otázku o dátach prieskumu SAFE…",
      send: "Opýtať sa",
      loading: "Premýšľam…",
      costLabel: "Cena: ${cost}",
      errorGeneric: "Pri odpovedaní na túto otázku sa niečo pokazilo.",
      errorRateLimit: "Dosiahli ste limit otázok — skúste to znova neskôr.",
      noTableData: "Pre túto odpoveď nie sú k dispozícii žiadne tabuľkové dáta.",
      navLink: "Opýtať sa na dáta SAFE",
      introDescription:
        "Položte otázku v bežnom jazyku o prieskume ECB SAFE — podmienky financovania, obchodná situácia, žiadosti o úvery, očakávania a ďalšie. Odpoveď sa generuje na základe reálneho dopytu do dát, pričom SQL aj cena sú zobrazené kvôli transparentnosti.",
      introDimensionsTitle: "Skúste porovnať podľa:",
      introDimensionCountry: "Krajiny — Slovensko vs eurozóna, alebo vs konkrétna krajina ako Nemecko",
      introDimensionFirmSize: "Veľkosť firmy — všetky firmy vs len MSP vs veľké firmy",
      introDimensionTime: "Čas — vývoj za posledných niekoľko vĺn, nielen za poslednú",
      introTrendingTitle: "Aktuálne trendy v poslednej vlne",
      introTrendingWorsened: "zhoršenie o {delta}pb od predchádzajúcej vlny",
      introTrendingImproved: "zlepšenie o {delta}pb od predchádzajúcej vlny",
      introExamplesTitle: "Príklady otázok",
      introExampleCountry: "Ako sa miera zamietnutia bankových úverov na Slovensku porovnáva s eurozónou?",
      introExampleFirmSize: "Líši sa medzera vo financovaní pre MSP a veľké firmy na Slovensku?",
      introExampleTrend: "Ako sa zmenil výhľad investícií na Slovensku za posledné 4 vlny?",
      showSql: "Zobraziť SQL",
    },
  },
} as const;

export type NewsletterId = keyof typeof STRINGS["en"]["newsletters"];
