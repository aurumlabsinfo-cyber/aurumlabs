import path from "node:path";

import type { NextConfig } from "next";

// Turbopack is the default builder in Next 16 for both dev and build; there is
// no flag to pass and no webpack config here, deliberately.
const nextConfig: NextConfig = {
  reactStrictMode: true,

  // This app lives inside a repository that has its own Next project at the
  // top level. Without an explicit root, Turbopack infers one from the nearest
  // lockfiles, walks up, and applies the parent project's PostCSS/Tailwind
  // config to these stylesheets — which fails, because this app deliberately
  // has no Tailwind dependency.
  turbopack: {
    root: path.resolve(__dirname),
  },

  env: {
    // The backend the dashboard reads. Overridable at build time so the same
    // source can point at a different host.
    NEXT_PUBLIC_API_BASE: process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8002",
  },
};

export default nextConfig;
