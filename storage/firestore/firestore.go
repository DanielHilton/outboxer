package firestore

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/italolelis/outboxer"
	"google.golang.org/api/option"
)

const (
	// DefaultEventStoreCollection is the default collection name.
	DefaultEventStoreCollection = "event_store"
)

type Firestore struct {
	client   *firestore.Client
	isLocked bool

	CollectionName string
}

// WithInstance creates a firestore data store with the required information to make a connection..
func WithInstance(ctx context.Context, projectID string, collectionName string, firestoreClientOptions ...option.ClientOption) (*Firestore, error) {
	c, err := firestore.NewClient(ctx, projectID, firestoreClientOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect firestore: %w", err)
	}

	f := Firestore{client: c}

	if len(collectionName) == 0 {
		f.CollectionName = DefaultEventStoreCollection
	} else {
		f.CollectionName = collectionName
	}

	return &f, nil
}

func (f *Firestore) Close() error {
	if err := f.client.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	return nil
}

func (f *Firestore) GetEvents(ctx context.Context, batchSize int32) ([]*outboxer.OutboxMessage, error) {
	//TODO implement me
	panic("implement me")
}

func (f *Firestore) Add(ctx context.Context, m *outboxer.OutboxMessage) error {
	//TODO implement me
	panic("implement me")
}

func (f *Firestore) AddWithinTx(ctx context.Context, m *outboxer.OutboxMessage, fn func(outboxer.ExecerContext) error) error {
	//TODO implement me
	panic("implement me")
}

func (f *Firestore) SetAsDispatched(ctx context.Context, id int64) error {
	//TODO implement me
	panic("implement me")
}

func (f *Firestore) Remove(ctx context.Context, since time.Time, batchSize int32) error {
	//TODO implement me
	panic("implement me")
}
