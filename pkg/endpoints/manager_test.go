package endpoints

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAwaitBestHost(t *testing.T) {
	const myService = "myService"
	const myPort = "myPort"

	manager := &Manager{endpoints: make(map[string]*endpointGroup, 1)}
	manager.getEndpoints(myService).
		setIPs(map[string]struct{}{myService: {}}, map[string]int32{myPort: 1})

	testCases := map[string]struct {
		service  string
		portName string
		timeout  time.Duration
		expErr   bool
	}{
		"all good": {
			service:  myService,
			portName: myPort,
			timeout:  time.Millisecond,
		},
		"unknown port - returns default if only 1": {
			service:  myService,
			portName: "unknownPort",
			timeout:  time.Millisecond,
		},
		"unknown service - blocks until timeout": {
			service:  "unknownService",
			portName: myPort,
			timeout:  time.Millisecond,
			expErr:   true,
		},
		// not covered: unknown port with multiple ports on entrypoint
	}

	for name, spec := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), spec.timeout)
			defer cancel()

			gotHost, gotErr := manager.AwaitHostAddress(ctx, spec.service, spec.portName)
			if spec.expErr {
				require.Error(t, gotErr)
				return
			}
			require.NoError(t, gotErr)
			assert.Equal(t, myService+":1", gotHost)
		})
	}
}
