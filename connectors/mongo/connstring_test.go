/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMongoClient_InvalidConnectionStringErrorHidesCredentials(t *testing.T) {
	for _, uri := range []string{
		"mongodb://user:a%PWb@127.0.0.1:1/",
		"mongodb://user:PW@127.0.0.1:1/?tls=PW",
		"mongodb://user:PW@127.0.0.1:1/?tls=true&tlsCAFile=/nonexistent/PW/ca.pem",
	} {
		_, err := MongoClient(t.Context(), ConnectorSettings{ConnectionString: uri})
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "PW")
	}
}
