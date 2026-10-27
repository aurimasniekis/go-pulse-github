package githubapi

import "encoding/json"

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// SplitArray splits a JSON array into its raw elements without decoding them (numbers and unknown fields stay exact).
func SplitArray(b []byte) ([]json.RawMessage, error) {
	var out []json.RawMessage
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}
