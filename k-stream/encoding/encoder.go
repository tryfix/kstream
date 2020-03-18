/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package encoding

type Builder func() Encoder

type Encoder interface {
	Encode(data interface{}) ([]byte, error)
	Decode(data []byte) (interface{}, error)
}
