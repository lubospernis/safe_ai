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
    },
  },
} as const;

export type NewsletterId = keyof typeof STRINGS["en"]["newsletters"];
