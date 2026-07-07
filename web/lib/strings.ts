export type Lang = "en" | "sk";

export const STRINGS = {
  en: {
    subscriptionService: "Newsletter subscription service",
    signOut: "Sign out",
    subscribe: "Subscribe",
    unsubscribe: "Unsubscribe",
    somethingWentWrong: "Something went wrong",
    formatDescription:
      "Sent to regulators every quarter, with an occasional extra ad-hoc newsletter for special survey rounds.",
    lastUpdated: "Updated {date}",
    nextRelease: "Next release: {date}*",
    nextReleaseFootnote: "*Newsletter will be sent out around this date.",
    newsletters: {
      "safe-slovakia": {
        name: "SAFE Slovakia",
        description:
          "Quarterly ECB Survey on the Access to Finance of Enterprises — Slovakia focus. Covers financing conditions, loan applications, business situation, and forward-looking expectations.",
        periodicity: "Quarterly",
      },
    },
  },
  sk: {
    subscriptionService: "Služba odberu newslettra",
    signOut: "Odhlásiť sa",
    subscribe: "Odoberať",
    unsubscribe: "Zrušiť odber",
    somethingWentWrong: "Niečo sa pokazilo",
    formatDescription:
      "Zasielané regulátorom každý štvrťrok, príležitostne aj mimoriadny newsletter pri špeciálnych kolách prieskumu.",
    lastUpdated: "Aktualizované {date}",
    nextRelease: "Ďalšie vydanie: {date}*",
    nextReleaseFootnote: "*Newsletter bude odoslaný približne v tomto termíne.",
    newsletters: {
      "safe-slovakia": {
        name: "SAFE Slovensko",
        description:
          "Štvrťročný prieskum ECB o prístupe firiem k financovaniu — zameranie na Slovensko. Zahŕňa podmienky financovania, žiadosti o úvery, obchodnú situáciu a výhľad do budúcnosti.",
        periodicity: "Štvrťročne",
      },
    },
  },
} as const;

export type NewsletterId = keyof typeof STRINGS["en"]["newsletters"];
