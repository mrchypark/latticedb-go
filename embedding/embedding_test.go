package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestHashMatchesUpstreamFixtures(t *testing.T) {
	tests := []struct {
		text string
		want []uint32
	}{
		{"hello world", []uint32{0, 0, 0, 0, 0, 0x3f3504f3, 0, 0xbf3504f3}},
		{"The QUICK brown_fox and a 123", []uint32{0xbee4f92e, 0, 0, 0, 0, 0, 0, 0x3f64f92e}},
		{"x y __ 0123456789", make([]uint32, 8)},
		{"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz", make([]uint32, 8)},
	}
	for _, test := range tests {
		vector, err := Hash(test.text, 8)
		if err != nil {
			t.Fatalf("Hash(%q): %v", test.text, err)
		}
		got := make([]uint32, len(vector))
		for index, value := range vector {
			got[index] = math.Float32bits(value)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("Hash(%q) bits = %08x, want %08x", test.text, got, test.want)
		}
	}
}

func TestHashDefaultsAndValidation(t *testing.T) {
	vector, err := Hash("hello", 0)
	if err != nil || len(vector) != defaultDimensions {
		t.Fatalf("Hash default = len %d, err %v", len(vector), err)
	}
	if _, err := Hash("", 8); err == nil {
		t.Fatal("Hash empty input succeeded")
	}
	if _, err := Hash("hello", maxDimensions+1); err == nil {
		t.Fatal("Hash oversized dimensions succeeded")
	}
}

func TestHashSupportsUnicodeWords(t *testing.T) {
	left, err := Hash("안녕하세요 세계", 32)
	if err != nil {
		t.Fatal(err)
	}
	right, err := Hash("데이터베이스 검색", 32)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(left, make([]float32, 32)) || reflect.DeepEqual(left, right) {
		t.Fatalf("unicode hashes are not distinct: %v / %v", left, right)
	}
}

func TestWyhashMatchesZigVectors(t *testing.T) {
	tests := []struct {
		seed  uint64
		input string
		want  uint64
	}{
		{0, "", 0x0409638ee2bde459},
		{1, "a", 0xa8412d091b5fe0a9},
		{2, "abc", 0x32dd92e4b2915153},
		{3, "message digest", 0x8619124089a3a16b},
		{4, "abcdefghijklmnopqrstuvwxyz", 0x7a43afb61d7f5f40},
		{5, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", 0xff42329b90e50d58},
		{6, "12345678901234567890123456789012345678901234567890123456789012345678901234567890", 0xc39cab13b115aad3},
	}
	for _, test := range tests {
		if got := wyhash(test.seed, []byte(test.input)); got != test.want {
			t.Fatalf("wyhash(%d, %q) = %x, want %x", test.seed, test.input, got, test.want)
		}
	}
}

func TestClientOllama(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("request = %s, content type %q, authorization %q", request.Method, request.Header.Get("Content-Type"), request.Header.Get("Authorization"))
		}
		var body map[string]string
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(body, map[string]string{"model": "model", "prompt": "hello"}) {
			t.Fatalf("request body = %#v", body)
		}
		_, _ = response.Write([]byte(`{"embedding":[0.1,-2,3]}`))
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL, Model: "model", APIKey: "secret", TimeoutMS: 123})
	if err != nil {
		t.Fatal(err)
	}
	if client.client.Timeout != 123*time.Millisecond {
		t.Fatalf("timeout = %s", client.client.Timeout)
	}
	vector, err := client.Embed("hello")
	if err != nil || !reflect.DeepEqual(vector, []float32{0.1, -2, 3}) {
		t.Fatalf("Embed = %#v, %v", vector, err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Embed("hello"); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Embed error = %v", err)
	}
}

func TestClientEmbedsConcurrently(t *testing.T) {
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		arrived <- struct{}{}
		<-release
		_, _ = response.Write([]byte(`{"embedding":[1]}`))
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { _, err := client.Embed("first"); results <- err }()
	go func() { _, err := client.Embed("second"); results <- err }()
	for range 2 {
		select {
		case <-arrived:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("concurrent Embed calls were serialized")
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Close blocked on active Embed calls")
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestClientEmbedContextCancelsRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	defer server.Close()
	client, err := NewClient(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.EmbedContext(ctx, "hello"); !errors.Is(err, context.Canceled) {
		t.Fatalf("EmbedContext error = %v", err)
	}
}

func TestClientOpenAIAndValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(body, map[string]string{"model": "openai-model", "input": "hello"}) {
			t.Fatalf("request body = %#v", body)
		}
		_, _ = response.Write([]byte(`{"data":[{"embedding":[1,2.5]}]}`))
	}))
	defer server.Close()

	client, err := NewEmbeddingClient(Config{Endpoint: server.URL, Model: "openai-model", APIFormat: APIFormatOpenAI})
	if err != nil {
		t.Fatal(err)
	}
	vector, err := client.Embed("hello")
	if err != nil || !reflect.DeepEqual(vector, []float32{1, 2.5}) {
		t.Fatalf("Embed = %#v, %v", vector, err)
	}
	if _, err := NewClient(Config{Endpoint: "relative"}); err == nil {
		t.Fatal("relative endpoint accepted")
	}
	if _, err := NewClient(Config{Endpoint: server.URL, APIFormat: APIFormat(99)}); err == nil {
		t.Fatal("unknown API format accepted")
	}

	failing := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Error(response, "no", http.StatusUnauthorized)
	}))
	defer failing.Close()
	client, err = NewClient(Config{Endpoint: failing.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Embed("hello"); err == nil {
		t.Fatal("non-200 response accepted")
	}
}
