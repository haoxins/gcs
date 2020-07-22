package gcp

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/pkg4go/tools"
	"google.golang.org/api/iterator"
)

// BigQueryClient The wrapper of BigQuery client
type BigQueryClient struct {
	ProjectID string
}

// GetRowsFromSQL Get the result rows from SQL
func (c *BigQueryClient) GetRowsFromSQL(sql string) [][]bigquery.Value {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, c.ProjectID)
	tools.AssertError(err)
	defer client.Close()

	q := client.Query(sql)
	iter, err := q.Read(ctx)
	tools.AssertError(err)

	var rows [][]bigquery.Value
	for {
		var row []bigquery.Value

		err := iter.Next(&row)
		if err == iterator.Done {
			break
		}

		tools.AssertError(err)

		rows = append(rows, row)
	}

	return rows
}
