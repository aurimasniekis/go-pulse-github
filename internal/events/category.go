package events

// knownCategories bounds metric labels for GitHub event names.
var knownCategories = map[string]bool{
	"push": true, "create": true, "delete": true, "issues": true, "issue_comment": true, "pull_request": true,
	"pull_request_review": true, "pull_request_review_comment": true, "pull_request_review_thread": true,
	"commit_comment": true, "release": true, "discussion": true, "discussion_comment": true, "workflow_run": true,
	"workflow_job": true, "workflow_dispatch": true, "check_run": true, "check_suite": true, "deployment": true,
	"deployment_status": true, "star": true, "watch": true, "fork": true, "repository": true, "member": true,
	"membership": true, "team": true, "team_add": true, "organization": true, "installation": true,
	"installation_repositories": true, "installation_target": true, "ping": true, "label": true, "milestone": true,
	"public": true, "gollum": true, "status": true, "sponsorship": true, "github_app_authorization": true,
	"thread": true, "issue": true, "pull": true, "coverage": true, "access": true, "doctor": true,
	"PushEvent": true, "audit_log": true,
}

// Category maps an event name to a bounded metric label ("other" for anything unknown).
func Category(event string) string {
	if knownCategories[event] {
		return event
	}
	return "other"
}
