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
    experimentalBadge: "Experimental",
    reportUnavailable: "Report data is not available yet.",
    reportLoadError: "Unable to load report content.",
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
    experimentalBadge: "Experimentálne",
    reportUnavailable: "Dáta správy ešte nie sú dostupné.",
    reportLoadError: "Nepodarilo sa načítať obsah správy.",
  },
} as const;
