export interface Newsletter {
  id: string;
  name: string;
  icon: string;
  description: string;
  periodicity: string;
}

export const NEWSLETTERS: Newsletter[] = [
  {
    id: "safe-regular",
    name: "SAFE Slovakia",
    icon: "🏦",
    description:
      "Quarterly ECB Survey on the Access to Finance of Enterprises — Slovakia focus. Covers financing conditions, loan applications, business situation, and forward-looking expectations.",
    periodicity: "Quarterly",
  },
  {
    id: "safe-adhoc",
    name: "SAFE Slovakia — Special Focus",
    icon: "🔦",
    description:
      "Ad-hoc deep dive on a special survey topic (e.g. AI adoption, green transition), sent whenever the ECB adds a one-off module to the SAFE survey.",
    periodicity: "Ad hoc",
  },
];
