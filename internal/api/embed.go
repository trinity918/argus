package api

import _ "embed"

// dashboardHTML is the self-contained single-page dashboard, embedded into the
// binary so the API service has no static-asset dependency to deploy.
//
//go:embed dashboard.html
var dashboardHTML []byte
