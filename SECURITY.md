# Security policy

Thyphon is a local laboratory. Do not expose its Compose ports or use real payment credentials.
Copy `.env.example` to a gitignored `.env`, replace its values, and use a managed secret store outside
local development. Provider callbacks require an HMAC over a canonical intention-bound body and a
five-minute timestamp window. A production integration should additionally use the provider's nonce
or event identifier as a durable replay key.
Report vulnerabilities privately to the maintainer.
