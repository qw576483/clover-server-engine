// Package json 提供统一 JSON 序列化/反序列化封装。
package json

import (
	stdjson "encoding/json"
	"fmt"
)

// Marshal 统一 JSON 序列化，屏蔽原生 json 包细节，错误做统一包装。
func Marshal(v any) ([]byte, error) {
	b, err := stdjson.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal: %w", err)
	}
	return b, nil
}

// Unmarshal 统一 JSON 反序列化，错误做统一包装。
func Unmarshal(data []byte, v any) error {
	if err := stdjson.Unmarshal(data, v); err != nil {
		return fmt.Errorf("json.Unmarshal: %w", err)
	}
	return nil
}
