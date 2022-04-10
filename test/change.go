package test

import (
	"encoding/json"
	"io"
)

type ChangeCounterInc struct {
	Value int `json:"value"`
}

func (c *ChangeCounterInc) TypeName() string {
	return "counter-inc"
}

func (c *ChangeCounterInc) ReadFrom(r io.Reader) (int64, error) {
	return 0, json.NewDecoder(r).Decode(c)
}

func (c *ChangeCounterInc) WriteTo(w io.Writer) (int64, error) {
	return 0, json.NewEncoder(w).Encode(c)
}

type ChangeAttachPayload struct {
	PayloadID string `json:"payloadID"`
}

func (c *ChangeAttachPayload) TypeName() string {
	return "attach-payload"
}

func (c *ChangeAttachPayload) ReadFrom(r io.Reader) (int64, error) {
	return 0, json.NewDecoder(r).Decode(c)
}

func (c *ChangeAttachPayload) WriteTo(w io.Writer) (int64, error) {
	return 0, json.NewEncoder(w).Encode(c)
}
