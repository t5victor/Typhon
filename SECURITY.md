# Security

Thyphon is a local systems laboratory. Do not expose its Compose ports or use
production payment credentials, broker credentials or API keys.

Local configuration belongs in an untracked `.env` file. Keep it in a managed
secret store outside the workstation when sharing access. Payment callbacks use
an HMAC over the canonical command body and a five-minute timestamp window; a
production provider integration also needs a durable nonce or provider event ID
to prevent long-lived replay.

Kafka records are treated as delivery input, not as authority. Consumers verify
each envelope against PostgreSQL before projecting it or starting Settlement.
Production deployments still require TLS/SASL, ACLs, separate database roles
and infrastructure-level network controls.

Report vulnerabilities privately to the maintainer. Do not open a public issue
with exploit details or credentials.
