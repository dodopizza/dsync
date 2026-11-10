package dsync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlowNameFlagNamesTheConnectors(t *testing.T) {
	// --cleanup stops right after the flow is resolved; the built-in connectors need no database.
	args := []string{"dsync", "--verbosity", "DEBUG", "--web-port", "0", "--flow-name", "cdp/dsync-prod-order-v2", "--cleanup", "/dev/random", "/dev/null"}

	output, err := captureOutput(t, func() error { return NewApp().Run(args) })

	require.NoError(t, err)
	assert.Contains(t, output, "cdp/dsync-prod-order-v2-Source")
	assert.Contains(t, output, "cdp/dsync-prod-order-v2-Destination")
}
