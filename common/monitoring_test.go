package common
import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestGetOrGenerateRequestID(t *testing.T) {
	// Test getting existing request ID
	existingID := "test-id"
	ctx := context.WithValue(context.Background(), requestIDKey, existingID)
	newCtx, id := GetOrGenerateRequestID(ctx)
	assert.Equal(t, existingID, id)
	assert.Equal(t, existingID, newCtx.Value(requestIDKey))

	// Test generating new request ID
	ctx = context.Background()
	newCtx, id = GetOrGenerateRequestID(ctx)
	assert.NotEmpty(t, id)
	assert.Equal(t, id, newCtx.Value(requestIDKey))
}

func TestGetRequestID(t *testing.T) {
	// Test getting existing request ID
	existingID := "test-id"
	ctx := context.WithValue(context.Background(), requestIDKey, existingID)
	id := GetRequestID(ctx)
	assert.Equal(t, existingID, id)

	// Test getting non-existent request ID
	ctx = context.Background()
	id = GetRequestID(ctx)
	assert.Empty(t, id)
}

func TestGetOrGenerateRequestIDAtServer(t *testing.T) {
	// Test getting existing request ID from metadata
	existingID := "test-id"
	md := metadata.New(map[string]string{requestIDHeader: existingID})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	newCtx, id := GetOrGenerateRequestIDAtServer(ctx)
	assert.Equal(t, existingID, id)

	// Test generating new request ID when metadata is empty
	ctx = context.Background()
	newCtx, id = GetOrGenerateRequestIDAtServer(ctx)
	assert.NotEmpty(t, id)
	assert.Equal(t, id, newCtx.Value(requestIDKey))
}

func TestSetClientRequestID(t *testing.T) {
	// Test with existing request ID in outgoing metadata
	existingID := "test-id"
	md := metadata.New(map[string]string{requestIDHeader: existingID})
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	newCtx, id := SetClientRequestID(ctx)
	assert.Equal(t, existingID, id)
	mdOut, _ := metadata.FromOutgoingContext(newCtx)
	assert.Equal(t, existingID, mdOut.Get(requestIDHeader)[0])

	// Test with existing request ID in context
	ctx = context.WithValue(context.Background(), requestIDKey, existingID)
	newCtx, id = SetClientRequestID(ctx)
	assert.Equal(t, existingID, id)
	mdOut, _ = metadata.FromOutgoingContext(newCtx)
	assert.Equal(t, existingID, mdOut.Get(requestIDHeader)[0])

	// Test generating new request ID
	ctx = context.Background()
	newCtx, id = SetClientRequestID(ctx)
	assert.NotEmpty(t, id)
	mdOut, _ = metadata.FromOutgoingContext(newCtx)
	assert.Equal(t, id, mdOut.Get(requestIDHeader)[0])
}
