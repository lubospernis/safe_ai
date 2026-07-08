import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // @duckdb/node-api loads a platform-specific native binding via a runtime
  // process.platform/arch switch (@duckdb/node-bindings/duckdb.js). Next's default
  // bundling tries to statically resolve every branch of that switch at build time,
  // which fails for platforms other than the one actually installed. Opting these
  // packages out of bundling makes Next use native Node `require` instead, so only
  // the branch that matches the real runtime platform is ever touched.
  serverExternalPackages: ["@duckdb/node-api", "@duckdb/node-bindings"],

  // The above stops Next from mangling the require() calls, but each platform's
  // node-bindings package also ships a *sibling* libduckdb.so that the .node
  // addon dlopen()s at runtime — Next's static file tracing can't see that
  // dynamic-link dependency, so it gets left out of the deployed function
  // bundle entirely. Confirmed in production: "Error: libduckdb.so: cannot
  // open shared object file" on every route that imports @duckdb/node-api.
  // Explicitly include the whole bindings package tree (both files) for every
  // route that touches DuckDB.
  outputFileTracingIncludes: {
    "/chat": ["./node_modules/@duckdb/node-bindings-*/**/*"],
    "/api/chat": ["./node_modules/@duckdb/node-bindings-*/**/*"],
    "/api/smoke-test": ["./node_modules/@duckdb/node-bindings-*/**/*"],
  },
};

export default nextConfig;
