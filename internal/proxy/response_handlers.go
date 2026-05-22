package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func (ps *ProxyServer) handleStreamingResponse(c *gin.Context, resp *http.Response) int64 {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		logrus.Error("Streaming unsupported by the writer, falling back to normal response")
		return ps.handleNormalResponse(c, resp)
	}

	buf := make([]byte, 4*1024)
	var responseBody bytes.Buffer
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			responseBody.Write(buf[:n])
			if _, writeErr := c.Writer.Write(buf[:n]); writeErr != nil {
				logUpstreamError("writing stream to client", writeErr)
				return extractTotalTokens(responseBody.Bytes())
			}
			flusher.Flush()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			logUpstreamError("reading from upstream", err)
			return extractTotalTokens(responseBody.Bytes())
		}
	}
	return extractTotalTokens(responseBody.Bytes())
}

func (ps *ProxyServer) handleNormalResponse(c *gin.Context, resp *http.Response) int64 {
	var responseBody bytes.Buffer
	teeReader := io.TeeReader(resp.Body, &responseBody)
	if _, err := io.Copy(c.Writer, teeReader); err != nil {
		logUpstreamError("copying response body", err)
	}
	return extractTotalTokens(responseBody.Bytes())
}

func extractTotalTokens(body []byte) int64 {
	if len(body) == 0 {
		return 0
	}

	if tokens := extractTotalTokensFromJSON(body); tokens > 0 {
		return tokens
	}
	return extractTotalTokensFromSSE(body)
}

func extractTotalTokensFromJSON(body []byte) int64 {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0
	}
	return totalTokensFromPayload(payload)
}

func extractTotalTokensFromSSE(body []byte) int64 {
	var latestTokens int64
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}

		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}
		if tokens := extractTotalTokensFromJSON([]byte(data)); tokens > 0 {
			latestTokens = tokens
		}
	}
	return latestTokens
}

func totalTokensFromPayload(payload map[string]any) int64 {
	if usage, ok := payload["usage"].(map[string]any); ok {
		if tokens := numericInt64(usage["total_tokens"]); tokens > 0 {
			return tokens
		}
	}

	if usage, ok := payload["usageMetadata"].(map[string]any); ok {
		if tokens := numericInt64(usage["totalTokenCount"]); tokens > 0 {
			return tokens
		}
	}
	return 0
}

func numericInt64(value any) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case json.Number:
		parsed, _ := v.Int64()
		return parsed
	case string:
		parsed, _ := strconv.ParseInt(v, 10, 64)
		return parsed
	default:
		return 0
	}
}

func extractThinkingDepth(body []byte) string {
	if len(body) == 0 {
		return ""
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}

	if value := stringValue(payload["reasoning_effort"]); value != "" {
		return value
	}
	if value := stringValue(payload["thinking_depth"]); value != "" {
		return value
	}
	if value := stringValue(payload["thinkingDepth"]); value != "" {
		return value
	}

	if reasoning, ok := payload["reasoning"].(map[string]any); ok {
		if value := stringValue(reasoning["effort"]); value != "" {
			return value
		}
		if value := stringValue(reasoning["summary"]); value != "" {
			return fmt.Sprintf("summary:%s", value)
		}
	}

	if thinking, ok := payload["thinking"].(map[string]any); ok {
		if value := stringValue(thinking["type"]); value != "" {
			return value
		}
		if tokens := numericInt64(thinking["budget_tokens"]); tokens > 0 {
			return fmt.Sprintf("budget:%d", tokens)
		}
	}

	if thinkingConfig, ok := payload["thinkingConfig"].(map[string]any); ok {
		if tokens := numericInt64(thinkingConfig["thinkingBudget"]); tokens > 0 {
			return fmt.Sprintf("budget:%d", tokens)
		}
		if enabled, ok := thinkingConfig["includeThoughts"].(bool); ok {
			return fmt.Sprintf("includeThoughts:%t", enabled)
		}
	}

	return ""
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case bool:
		return strconv.FormatBool(v)
	default:
		return ""
	}
}
