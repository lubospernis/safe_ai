export interface Newsletter {
  id: string;
  name: string;
  icon: string;
  description: string;
  periodicity: string;
}

export const NEWSLETTERS: Newsletter[] = [
  {
    id: "safe-slovakia",
    name: "SAFE Slovakia",
    icon: "🏦",
    description:
      "Quarterly ECB Survey on the Access to Finance of Enterprises — Slovakia focus. Covers financing conditions, loan applications, business situation, and forward-looking expectations.",
    periodicity: "Quarterly",
  },
];
