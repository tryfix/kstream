/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package processors

import (
	"context"
	"testing"
)

var p ProcessFunc = func(ctx context.Context, key interface{}, value interface{}) error {
	return nil
}

func TestProcessFunc_Process(t *testing.T) {
	if err := p(context.Background(), nil, nil); err != nil {
		t.Fail()
	}
}
