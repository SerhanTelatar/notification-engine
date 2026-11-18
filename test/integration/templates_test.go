//go:build integration

package integration

import (
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

// US8: create, read and list templates.
func TestTemplatesCRUD(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	u := api.URL + "/api/v1/templates"

	res := postJSON(t, u, `{"name":"otp","body":"Hi {{.Name}}, your code is {{.Code}}"}`, nil)
	if res.code != http.StatusCreated {
		t.Fatalf("create: %d %v", res.code, res.body)
	}
	id, _ := res.body["id"].(string)
	vars, _ := res.body["variables"].([]any)
	if id == "" || len(vars) != 2 || vars[0] != "Code" || vars[1] != "Name" {
		t.Fatalf("create body %v: want sorted variables [Code Name]", res.body)
	}

	if res := postJSON(t, u, `{"name":"otp","body":"other"}`, nil); res.code != http.StatusConflict || errorCode(res) != "template_name_taken" {
		t.Fatalf("duplicate name: %d %v", res.code, res.body)
	}
	if res := postJSON(t, u, `{"name":"loop","body":"{{range .Items}}x{{end}}"}`, nil); res.code != http.StatusBadRequest || !slices.Equal(errorFields(res), []string{"body"}) {
		t.Fatalf("bad syntax: %d %v", res.code, res.body)
	}

	if code, body := getJSON(t, u+"/"+id); code != http.StatusOK || body["name"] != "otp" || body["body"] != "Hi {{.Name}}, your code is {{.Code}}" {
		t.Fatalf("get: %d %v", code, body)
	}
	if code, _ := getJSON(t, u+"/0190f4a0-0000-7000-8000-000000000001"); code != http.StatusNotFound {
		t.Fatalf("unknown template: %d", code)
	}
	if code, body := getJSON(t, u); code != http.StatusOK || len(body["items"].([]any)) != 1 {
		t.Fatalf("list: %d %v", code, body)
	}
}

// US8-AS1..4: sending by template delivers the rendered text; problems are 400s on the request.
func TestSendByTemplate(t *testing.T) {
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)
	startPipeline(t, defaultPipeline(mock.SendURL()))
	api := newAPI(t)

	res := postJSON(t, api.URL+"/api/v1/templates", `{"name":"otp","body":"Hi {{.Name}}, your code is {{.Code}}"}`, nil)
	tplID, _ := res.body["id"].(string)
	notifications := api.URL + "/api/v1/notifications"

	res = postJSON(t, notifications, `{"recipient":"+905551234567","channel":"sms","template_id":"`+tplID+`","variables":{"Name":"Ada","Code":"1234","Unused":"x"}}`, nil)
	if res.code != http.StatusAccepted {
		t.Fatalf("send by template: %d %v", res.code, res.body)
	}
	id, _ := res.body["id"].(string)
	waitFor(t, 10*time.Second, "sent", func() bool { st, _ := statusOf(t, id); return st == "sent" })
	got := mock.Received()
	if len(got) != 1 || got[0].Content != "Hi Ada, your code is 1234" {
		t.Fatalf("provider received %+v, want the rendered text", got)
	}

	cases := []struct {
		name, body string
		fields     []string
	}{
		{"missing variables", `{"recipient":"+905551234567","channel":"sms","template_id":"` + tplID + `","variables":{"Name":"Ada"}}`, []string{"variables.Code"}},
		{"unknown template", `{"recipient":"+905551234567","channel":"sms","template_id":"0190f4a0-0000-7000-8000-000000000001"}`, []string{"template_id"}},
		{"content and template", `{"recipient":"+905551234567","channel":"sms","content":"hi","template_id":"` + tplID + `"}`, []string{"content"}},
	}
	for _, tc := range cases {
		res := postJSON(t, notifications, tc.body, nil)
		if res.code != http.StatusBadRequest || !slices.Equal(errorFields(res), tc.fields) {
			t.Fatalf("%s: %d %v, want 400 on %v", tc.name, res.code, res.body, tc.fields)
		}
	}

	// A batch item can use a template too.
	res = postJSON(t, notifications+"/batch", `{"items":[
		{"recipient":"+905551234568","channel":"sms","template_id":"`+tplID+`","variables":{"Name":"Bo","Code":"9"}},
		{"recipient":"+905551234569","channel":"sms","template_id":"`+tplID+`"}]}`, nil)
	if res.code != http.StatusAccepted || res.body["accepted"] != float64(1) || res.body["rejected"] != float64(1) {
		t.Fatalf("batch: %d %v", res.code, res.body)
	}
	waitFor(t, 10*time.Second, "batch item sent", func() bool {
		for _, r := range mock.Received() {
			if r.Content == "Hi Bo, your code is 9" {
				return true
			}
		}
		return false
	})
}
