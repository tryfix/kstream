/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package kstream

//import (
//	"github.com/tryfix/kstream/encoding"
//	"github.com/tryfix/kstream/logger"
//	"github.com/tryfix/kstream/kstream/store"
//)
//
//type KTable struct {
//	kStream
//	store store.Store
//}
//
//func NewKTable(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Option) Stream {
//	if keyEncoder == nil {
//		logger.DefaultLogger.Fatal(`k-stream.kStream`, `keyEncoder cannot be null`)
//	}
//
//	if valEncoder == nil {
//		logger.DefaultLogger.Fatal(`k-stream.kStream`, `valEncoder cannot be null`)
//	}
//
//	return newKStream(func(s string) string { return topic }, keyEncoder, valEncoder, nil, options...)
//}
