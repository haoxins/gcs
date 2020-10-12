package gcp

import (
	"context"
	"strings"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"github.com/haoxins/tools/v2"
)

// FireStoreClient The fire store client
type FireStoreClient struct {
	ProjectID  string
	Collection string
}

// Get ...
func (c *FireStoreClient) Get(id string) *firestore.DocumentSnapshot {
	client, ctx := c.initFireStoreClient()
	defer client.Close()

	ds, err := client.Collection(c.Collection).Doc(id).Get(ctx)
	if err != nil {
		isNotFound := strings.Contains(err.Error(), "code = NotFound")

		if isNotFound {

			return nil
		}

		return nil
	}

	return ds
}

// Set ...
func (c *FireStoreClient) Set(id string, data interface{}) {
	client, ctx := c.initFireStoreClient()
	defer client.Close()

	_, err := client.Collection(c.Collection).Doc(id).Set(ctx, data)
	tools.AssertError(err)
}

func (c *FireStoreClient) initFireStoreClient() (*firestore.Client, context.Context) {
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, c.ProjectID)
	tools.AssertError(err)

	return client, ctx
}

// BatchInsert - Batch insert
func (c *FireStoreClient) BatchInsert(docs []interface{}) (allIds []string) {
	client, ctx := c.initFireStoreClient()
	defer client.Close()

	batch := client.Batch()
	var ids []string

	for _, doc := range docs {
		id := uuid.New().String()
		// Create
		ref := client.Collection(c.Collection).Doc(id)
		batch.Set(ref, doc)

		ids = append(ids, id)
	}

	// Commit
	_, err := batch.Commit(ctx)
	tools.AssertError(err)

	return ids
}
