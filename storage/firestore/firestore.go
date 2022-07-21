package firestore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/italolelis/outboxer"
	"google.golang.org/api/iterator"
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

type firestoreOutboxMessage struct {
	ID           int64                  `firestore:"id"`
	Dispatched   bool                   `firestore:"dispatched"`
	DispatchedAt time.Time              `firestore:"dispatched_at"`
	Payload      string                 `firestore:"payload"`
	Options      outboxer.DynamicValues `firestore:"options"`
	Headers      outboxer.DynamicValues `firestore:"headers"`
}

// WithInstance creates a firestore data store with the required information to make a connection.
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

// GetEvents retrieves all events for the batchSize which are not dispatched.
func (f *Firestore) GetEvents(ctx context.Context, batchSize int32) ([]*outboxer.OutboxMessage, error) {
	var events []*outboxer.OutboxMessage

	docs := f.client.Collection(f.CollectionName).Where("dispatched", "==", false).Limit(int(batchSize)).Documents(ctx)
	for {
		doc, err := docs.Next()
		if err != nil {
			if err == iterator.Done {
				return events, nil
			}

			return nil, fmt.Errorf("failed to get messages from store: %w", err)
		}

		var f firestoreOutboxMessage
		err = doc.DataTo(&f)
		if err != nil {
			return nil, fmt.Errorf("malformed event found, error: %w", err)
		}

		e := outboxer.OutboxMessage{
			ID:         f.ID,
			Dispatched: f.Dispatched,
			DispatchedAt: sql.NullTime{
				Time:  f.DispatchedAt,
				Valid: true,
			},
			Payload: []byte(f.Payload),
			Options: f.Options,
			Headers: f.Headers,
		}

		events = append(events, &e)
	}
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
