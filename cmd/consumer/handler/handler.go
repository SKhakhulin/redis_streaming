package handler

import (
	"github.com/streaming/test/pkg/event"
)

type Handler interface {
	Handle(e event.Event) error
}
