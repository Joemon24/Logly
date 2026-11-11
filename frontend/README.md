# Logly dashboard

The React dashboard uses the read-only FastAPI endpoints from Logly backend
`0272c84`. Event search matches the source IP, host, username, outcome and UTC
start date supported by the backend. The activity chart covers a rolling 24-hour
window and discloses the API limit of 500 records.

## Run locally

Start Elasticsearch and the existing FastAPI service using your Logly backend
setup. With the backend listening on `127.0.0.1:8000`, run these commands from
this directory.

```sh
npm ci
npm run dev
```

If FastAPI listens elsewhere, set `LOGLY_API_TARGET` when starting Vite. The
dashboard calls same-origin `/api` routes. Vite proxies them to FastAPI in dev
and preview.

```sh
LOGLY_API_TARGET=http://127.0.0.1:8001 npm run dev
```

## Verify

```sh
npm run typecheck
npm run lint
npm test
npm run build
```

In a browser, use the event filters, open an incident timeline, and follow its
host link. Repeat at a narrow viewport and use Tab to reach the navigation,
filters, table scroll regions, and event disclosures.
