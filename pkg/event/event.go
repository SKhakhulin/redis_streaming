package event

import "encoding"

type Type string

const (
	OrderType   Type = "OrderType"
)

type Event interface {
	GetID() string
	GetType() Type
	SetID(id string)
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}
