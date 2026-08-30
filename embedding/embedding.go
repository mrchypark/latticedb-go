// Package embedding provides optional deterministic and HTTP embedding helpers.
package embedding

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	defaultDimensions = 128
	maxDimensions     = 4096
	defaultModel      = "nomic-embed-text"
	defaultTimeoutMS  = 30_000
	maxResponseBytes  = 10 << 20
)

// APIFormat selects the request and response JSON shapes.
type APIFormat int

const (
	APIFormatOllama APIFormat = iota
	APIFormatOpenAI
)

// Config configures Client. Endpoint is required; zero Model and TimeoutMS use
// LatticeDB's nomic-embed-text and 30-second defaults.
type Config struct {
	Endpoint  string
	Model     string
	APIFormat APIFormat
	APIKey    string
	TimeoutMS uint32
}

// ErrClosed is returned when Embed is called after Close.
var ErrClosed = errors.New("embedding client is closed")

// Client calls an Ollama or OpenAI-compatible embedding endpoint.
type Client struct {
	mu     sync.RWMutex
	client *http.Client
	config Config
	closed bool
}

// Hash returns the deterministic upstream LatticeDB hash embedding.
func Hash(text string, dimensions uint16) ([]float32, error) {
	if text == "" {
		return nil, errors.New("embedding text is empty")
	}
	if dimensions == 0 {
		dimensions = defaultDimensions
	}
	if dimensions > maxDimensions {
		return nil, fmt.Errorf("embedding dimensions must be between 1 and %d", maxDimensions)
	}

	vector := make([]float32, dimensions)
	var lower [64]byte
	for offset := 0; offset < len(text); {
		for offset < len(text) && !hashWordByte(text[offset]) {
			offset++
		}
		start := offset
		for offset < len(text) && hashWordByte(text[offset]) {
			offset++
		}
		length := offset - start
		if length < 2 || length > len(lower) {
			continue
		}
		for index := range length {
			lower[index] = lowerASCII(text[start+index])
		}
		token := lower[:length]
		if hashStopWord(token) {
			continue
		}
		hash := wyhash(0, token)
		if hash>>63 == 0 {
			vector[hash%uint64(dimensions)]++
		} else {
			vector[hash%uint64(dimensions)]--
		}
	}

	var squared float32
	for _, value := range vector {
		squared += value * value
	}
	if squared != 0 {
		inverse := float32(1 / math.Sqrt(float64(squared)))
		for index := range vector {
			vector[index] *= inverse
		}
	}
	return vector, nil
}

// HashEmbed is a compatibility alias for Hash.
func HashEmbed(text string, dimensions uint16) ([]float32, error) { return Hash(text, dimensions) }

// NewClient creates an optional HTTP embedding client.
func NewClient(config Config) (*Client, error) {
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return nil, errors.New("embedding endpoint must be an absolute HTTP URL")
	}
	if config.APIFormat != APIFormatOllama && config.APIFormat != APIFormatOpenAI {
		return nil, errors.New("unsupported embedding API format")
	}
	if config.Model == "" {
		config.Model = defaultModel
	}
	if config.TimeoutMS == 0 {
		config.TimeoutMS = defaultTimeoutMS
	}
	return &Client{client: &http.Client{Timeout: time.Duration(config.TimeoutMS) * time.Millisecond}, config: config}, nil
}

// NewEmbeddingClient is a compatibility alias for NewClient.
func NewEmbeddingClient(config Config) (*Client, error) { return NewClient(config) }

// Close releases this client. It is safe to call more than once.
func (client *Client) Close() error {
	if client == nil {
		return nil
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.closed = true
	client.client.CloseIdleConnections()
	return nil
}

// Embed requests one vector from the configured endpoint.
func (client *Client) Embed(text string) ([]float32, error) {
	if client == nil {
		return nil, ErrClosed
	}
	if text == "" {
		return nil, errors.New("embedding text is empty")
	}
	client.mu.RLock()
	defer client.mu.RUnlock()
	if client.closed {
		return nil, ErrClosed
	}

	body, err := json.Marshal(client.request(text))
	if err != nil {
		return nil, fmt.Errorf("encode embedding request: %w", err)
	}
	request, err := http.NewRequest(http.MethodPost, client.config.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create embedding request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if client.config.APIKey != "" {
		request.Header.Set("Authorization", "Bearer "+client.config.APIKey)
	}
	response, err := client.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("embedding request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding server returned %s", response.Status)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read embedding response: %w", err)
	}
	if len(data) > maxResponseBytes {
		return nil, errors.New("embedding response exceeds size limit")
	}
	vector, err := client.parse(data)
	if err != nil {
		return nil, fmt.Errorf("parse embedding response: %w", err)
	}
	return vector, nil
}

func (client *Client) request(text string) any {
	if client.config.APIFormat == APIFormatOpenAI {
		return struct {
			Model string `json:"model"`
			Input string `json:"input"`
		}{client.config.Model, text}
	}
	return struct {
		Model  string `json:"model"`
		Prompt string `json:"prompt"`
	}{client.config.Model, text}
}

func (client *Client) parse(data []byte) ([]float32, error) {
	if client.config.APIFormat == APIFormatOpenAI {
		var response struct {
			Data []struct {
				Embedding []float32 `json:"embedding"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &response); err != nil || len(response.Data) == 0 || response.Data[0].Embedding == nil {
			return nil, errors.New("invalid OpenAI embedding response")
		}
		return response.Data[0].Embedding, nil
	}
	var response struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.Unmarshal(data, &response); err != nil || response.Embedding == nil {
		return nil, errors.New("invalid Ollama embedding response")
	}
	return response.Embedding, nil
}

func hashWordByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_'
}

func lowerASCII(value byte) byte {
	if value >= 'A' && value <= 'Z' {
		return value + ('a' - 'A')
	}
	return value
}

func hashStopWord(token []byte) bool {
	switch string(token) {
	case "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "have", "he", "in", "is", "it", "its", "of", "on", "or", "that", "the", "to", "was", "were", "will", "with", "this", "but", "they", "had", "not", "you", "which", "can", "if", "their", "said", "each", "she", "do", "how", "we", "so", "up", "out", "about", "who", "been", "would", "there", "what", "when", "your", "all", "no", "just", "more", "some", "into", "than", "could", "other", "then", "only", "over", "such", "our", "also", "may", "these", "after", "any", "most", "very", "where", "much", "should", "those", "being", "because", "before", "between", "both", "come", "did", "does", "done", "during", "get", "got", "him", "her", "here", "his", "i", "me", "my", "myself":
		return true
	}
	return false
}

func wyhash(seed uint64, input []byte) uint64 {
	const secret0 = uint64(0xa0761d6478bd642f)
	const secret1 = uint64(0xe7037ed1a0b428db)
	const secret2 = uint64(0x8ebc6af09c88c6e3)
	const secret3 = uint64(0x589965cc75374cc3)
	state0 := seed ^ wyMix(seed^secret0, secret1)
	var a, b uint64
	if len(input) <= 16 {
		if len(input) >= 4 {
			end := len(input) - 4
			quarter := (len(input) >> 3) << 2
			a = uint64(wyRead4(input))<<32 | uint64(wyRead4(input[quarter:]))
			b = uint64(wyRead4(input[end:]))<<32 | uint64(wyRead4(input[end-quarter:]))
		} else if len(input) > 0 {
			a = uint64(input[0])<<16 | uint64(input[len(input)>>1])<<8 | uint64(input[len(input)-1])
		}
	} else {
		index := 0
		if len(input) >= 48 {
			state1, state2 := state0, state0
			for index+48 < len(input) {
				state0 = wyMix(wyRead8(input[index:])^secret1, wyRead8(input[index+8:])^state0)
				state1 = wyMix(wyRead8(input[index+16:])^secret2, wyRead8(input[index+24:])^state1)
				state2 = wyMix(wyRead8(input[index+32:])^secret3, wyRead8(input[index+40:])^state2)
				index += 48
			}
			state0 ^= state1 ^ state2
		}
		for offset := index; offset+16 < len(input); offset += 16 {
			state0 = wyMix(wyRead8(input[offset:])^secret1, wyRead8(input[offset+8:])^state0)
		}
		a = wyRead8(input[len(input)-16:])
		b = wyRead8(input[len(input)-8:])
	}
	a ^= secret1
	b ^= state0
	low, high := bits.Mul64(a, b)
	a, b = high, low
	return wyMix(a^secret0^uint64(len(input)), b^secret1)
}

func wyRead4(value []byte) uint32 {
	return uint32(value[0]) | uint32(value[1])<<8 | uint32(value[2])<<16 | uint32(value[3])<<24
}

func wyRead8(value []byte) uint64 {
	return uint64(wyRead4(value)) | uint64(wyRead4(value[4:]))<<32
}

func wyMix(left, right uint64) uint64 {
	high, low := bits.Mul64(left, right)
	return high ^ low
}
