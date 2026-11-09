# Security Policy

## Supported Versions

The following table indicates the support status for versions of Apache Spark Learning Lab:

| Version | Supported          |
| ------- | ------------------ |
| 4.2.x   | Yes                |
| < 4.2   | No                 |

---

## Reporting a Vulnerability

The project maintainers take security seriously. If you discover a security vulnerability within this repository or container configuration, please report it responsibly.

### How to Report

1. Do NOT open a public GitHub issue.
2. Email the maintainer directly at: `elhussienysabry@gmail.com`.
3. Include the following details in your report:
   - A description of the vulnerability and its potential impact.
   - Clear steps to reproduce or a minimal proof of concept.
   - Affected components (for example, Docker configuration, Jupyter authentication, Spark network ports).
   - Any suggested mitigations or patches.

### Response Timeline

- Acknowledgement: Within 48 hours of receipt.
- Assessment and Initial Feedback: Within 5 business days.
- Fix and Disclosure: A patch will be prepared in a private fork and released alongside a security advisory.

---

## Security Best Practices for Users

When running this Spark cluster:
- Localhost Binding: All ports in `docker-compose.yml` are bound to `127.0.0.1` by default to prevent exposure to external networks. Do not bind ports to `0.0.0.0` on untrusted networks without adequate firewalling and authentication.
- Jupyter Authentication: Set a strong token in `.env` (`JUPYTER_TOKEN=<your-secure-token>`) instead of the default development token if exposing Jupyter beyond localhost.
- Spark Web UI: The Spark Web UI (port 8080) and Driver UI (port 4040) do not implement access control by default in Standalone mode; keep them on local networks.
