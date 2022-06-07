package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"sync"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

type NRQLResponse struct {
	Data struct {
		Actor struct {
			Nrql struct {
				Results []interface{} `json:"results"`
			} `json:"nrql"`
		} `json:"actor"`
	} `json:"data"`
}

type Request struct {
	GSSpreadsheetID string
	GSReadRange     string
	NRApiKey        string
	NRQuery         string
	NRAccountID     int64
}

func main() {
	ctx := context.Background()

	srv, err := sheets.NewService(ctx, option.WithCredentialsFile("secret.json"), option.WithScopes("https://www.googleapis.com/auth/spreadsheets"))
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
		return
	}

	beginTime := "2022-05-17 00:00:00 +0700"
	endTime := "2022-05-17 23:59:00 +0700"

	req := []Request{
		{
			GSSpreadsheetID: "1OJvTF-DluDqBf2hdyh6oe-_Wdr8EzB3l6AZugdOKIyA",
			GSReadRange:     "Play GRPC Report!A1:F1",
			NRAccountID:     3221984,
			NRApiKey:        "NRAK-B586QA7ATYPKCBYATQXK1QJF110",
			NRQuery:         "SELECT rate(sum(play_play_viewer_grpc.count.percentiles), 1 second) as 'RPS',percentage(sum(play_play_viewer_grpc.count.percentiles), WHERE status != 'error') AS 'Success Percentage', average(play_play_viewer_grpc.upper.percentiles) as 'Average Latency',max(play_play_viewer_grpc.upper.percentiles) as 'Maximum Latency' FROM Metric WHERE environment = 'production' facet rpcmethod LIMIT 2000",
		},
		{
			GSSpreadsheetID: "1OJvTF-DluDqBf2hdyh6oe-_Wdr8EzB3l6AZugdOKIyA",
			GSReadRange:     "Play Interactive GRPC Report!A1:F1",
			NRAccountID:     3221984,
			NRApiKey:        "NRAK-B586QA7ATYPKCBYATQXK1QJF110",
			NRQuery:         "SELECT rate(sum(play_interactive_play_interactive_grpc.count.percentiles), 1 second) as 'RPS',percentage(sum(play_interactive_play_interactive_grpc.count.percentiles), WHERE status != 'error') AS 'Success Percentage', average(play_interactive_play_interactive_grpc.upper.percentiles) as 'Average Latency',max(play_interactive_play_interactive_grpc.upper.percentiles) as 'Maximum Latency' FROM Metric WHERE environment = 'production' facet rpcmethod LIMIT 2000",
		},
		{
			GSSpreadsheetID: "1OJvTF-DluDqBf2hdyh6oe-_Wdr8EzB3l6AZugdOKIyA",
			GSReadRange:     "Play Infrastructure Report!A1:G1",
			NRAccountID:     3239512,
			NRApiKey:        "NRAK-B586QA7ATYPKCBYATQXK1QJF110",
			NRQuery:         "SELECT average(cpuPercent) AS 'Average CPU Percentage', Max(cpuPercent) AS 'Max CPU Usage Percentage', average(memoryUsedPercent) AS 'Average Memory Usage Percentage', max(memoryUsedPercent) AS 'Max Memory Usage Percentage'  FROM SystemSample FACET hostgroup LIMIT 2000 WHERE owner='content-marketing' WHERE hostgroup RLIKE 'play.*' or hostgroup RLIKE 'groupchat.*' AND environment='production'",
		},
	}

	wg := sync.WaitGroup{}
	errChan := make(chan error, len(req)+1)

	for _, r := range req {

		wg.Add(1)

		go func(r Request, errChan chan<- error) {
			defer wg.Done()

			err := generateReportToGoogleSheetFile(ctx, srv, beginTime, endTime, r.GSReadRange, r.GSSpreadsheetID, r.NRApiKey, r.NRQuery, r.NRAccountID)
			if err != nil {
				errChan <- err
			}
		}(r, errChan)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		log.Fatalln(err)
	}

}

func generateReportToGoogleSheetFile(ctx context.Context, srv *sheets.Service, beginTime string, endTime string, readRange string, spreadsheetId string, apiKey string, query string, accountID int64) error {

	if readRange == "" || spreadsheetId == "" || apiKey == "" || query == "" || beginTime == "" || endTime == "" {
		return errors.New("one of the parameter is empty")
	}

	resp, err := srv.Spreadsheets.Values.Get(spreadsheetId, readRange).Do()
	if err != nil {
		return err
	}

	var columns []string = make([]string, 0)
	if len(resp.Values) > 0 {
		for _, v := range resp.Values[0] {
			column := v.(string)
			columns = append(columns, column)
		}
	}
	apiKey = fmt.Sprintf("API-Key: %v", apiKey)

	since := fmt.Sprintf(`SINCE '%v' UNTIL '%v'`, beginTime, endTime)
	fullquery := query + since

	fullquery = strings.ReplaceAll(fullquery, `'`, `\u0027`)

	gql := fmt.Sprintf(`{"query":"{\n  actor {\n    nrql(query: \"%v\", accounts: %v) {\n      results\n    }\n  }\n}\n", "variables":""}`, fullquery, accountID)

	cmd := exec.CommandContext(ctx, "curl", "https://api.newrelic.com/graphql", "-H", "Content-Type: application/json", "-H", apiKey, "--data-binary", gql)
	out, err := cmd.Output()
	if err != nil {
		return err
	}

	var nrqlResponse NRQLResponse
	if err := json.Unmarshal(out, &nrqlResponse); err != nil {
		return err
	}

	data := [][]interface{}{}

	for _, v := range nrqlResponse.Data.Actor.Nrql.Results {
		m, ok := v.(map[string]interface{})
		if !ok {
			continue
		}

		row := []interface{}{}
		for _, col := range columns {
			if col == "endTime" {
				row = append(row, endTime)
				continue
			}

			if col == "beginTime" {
				row = append(row, beginTime)
				continue
			}

			row = append(row, m[col])
		}

		data = append(data, row)
	}

	rb := &sheets.ValueRange{
		Values: data,
	}

	newResp, err := srv.Spreadsheets.Values.Append(spreadsheetId, readRange, rb).ValueInputOption("USER_ENTERED").InsertDataOption("INSERT_ROWS").Context(ctx).Do()
	if err != nil {
		return err
	}

	if newResp.HTTPStatusCode != http.StatusOK {
		return fmt.Errorf("status not ok : %v", newResp.ServerResponse)
	}

	return nil
}
