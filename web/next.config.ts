import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // @duckdb/node-api loads a platform-specific native binding via a runtime
  // process.platform/arch switch (@duckdb/node-bindings/duckdb.js). Next's default
  // bundling tries to statically resolve every branch of that switch at build time,
  // which fails for platforms other than the one actually installed. Opting these
  // packages out of bundling makes Next use native Node `require` instead, so only
  // the branch that matches the real runtime platform is ever touched.
  serverExternalPackages: ["@duckdb/node-api", "@duckdb/node-bindings"],
};

export default nextConfig;
