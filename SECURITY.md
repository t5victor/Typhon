# Security policy

Thyphon is a local laboratory. Do not expose its Compose ports or use real payment credentials.
Copy `.env.example` to a gitignored `.env`, replace its values, and use a managed secret store outside
local development. Provider callbacks require an HMAC over a canonical intention-bound body, but this
is not a substitute for provider-specific timestamp/replay protection in a production integration.
Report vulnerabilities privately to the maintainer.
