package latticedb

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestDeprecatedEmbeddingCompatibility(t *testing.T) {
	first, err := HashEmbed("hello world", 8)
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashEmbed("hello world", 8)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("HashEmbed deterministic = %#v, %v", second, err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = response.Write([]byte(`{"embedding":[1]}`))
	}))
	defer server.Close()
	client, err := NewEmbeddingClient(EmbeddingConfig{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Embed("hello"); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Embed("hello"); !errors.Is(err, ErrEmbeddingClosed) {
		t.Fatalf("closed embedding client error = %v", err)
	}
}
