package latticedb

import (
	"errors"

	latticeembedding "github.com/mrchypark/latticedb-go/embedding"
)

// EmbeddingAPIFormat selects the wire format used by an embedding endpoint.
//
// Deprecated: use package github.com/mrchypark/latticedb-go/embedding.
type EmbeddingAPIFormat = latticeembedding.APIFormat

const (
	// Deprecated: use embedding.APIFormatOllama.
	EmbeddingAPIFormatOllama = latticeembedding.APIFormatOllama
	// Deprecated: use embedding.APIFormatOpenAI.
	EmbeddingAPIFormatOpenAI = latticeembedding.APIFormatOpenAI
)

// EmbeddingConfig configures an optional HTTP embedding client.
//
// Deprecated: use embedding.Config.
type EmbeddingConfig = latticeembedding.Config

// EmbeddingClient is an optional HTTP embedding client.
//
// Deprecated: use embedding.Client.
type EmbeddingClient struct{ client *latticeembedding.Client }

// ErrEmbeddingClosed is returned when Embed is called after Close.
//
// Deprecated: use embedding.ErrClosed.
var ErrEmbeddingClosed = errors.New("embedding client is closed")

// HashEmbed returns the deterministic built-in hash embedding.
//
// Deprecated: use embedding.Hash.
func HashEmbed(text string, dimensions uint16) ([]float32, error) {
	return latticeembedding.Hash(text, dimensions)
}

// NewEmbeddingClient creates an HTTP embedding client.
//
// Deprecated: use embedding.NewClient.
func NewEmbeddingClient(config EmbeddingConfig) (*EmbeddingClient, error) {
	client, err := latticeembedding.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &EmbeddingClient{client: client}, nil
}

// Embed requests one vector from the configured endpoint.
func (client *EmbeddingClient) Embed(text string) ([]float32, error) {
	if client == nil || client.client == nil {
		return nil, ErrEmbeddingClosed
	}
	vector, err := client.client.Embed(text)
	if errors.Is(err, latticeembedding.ErrClosed) {
		return nil, ErrEmbeddingClosed
	}
	return vector, err
}

// Close releases this client. It is safe to call more than once.
func (client *EmbeddingClient) Close() error {
	if client == nil || client.client == nil {
		return nil
	}
	return client.client.Close()
}
