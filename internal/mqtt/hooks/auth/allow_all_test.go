// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package auth

import (
	"testing"

	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/stretchr/testify/require"
)

func TestAllowAllID(t *testing.T) {
	h := new(AllowHook)
	require.Equal(t, "allow-all-auth", h.ID())
}

func TestAllowAllProvides(t *testing.T) {
	h := new(AllowHook)
	require.True(t, h.Provides(mqtt.OnACLCheck))
	require.True(t, h.Provides(mqtt.OnConnectAuthenticate))
	require.False(t, h.Provides(mqtt.OnPublish))
}

func TestAllowAllOnConnectAuthenticate(t *testing.T) {
	h := new(AllowHook)
	require.True(t, h.OnConnectAuthenticate(new(mqtt.Client), packets.Packet{}))
}

func TestAllowAllOnACLCheck(t *testing.T) {
	h := new(AllowHook)
	require.True(t, h.OnACLCheck(new(mqtt.Client), "any", true))
}
