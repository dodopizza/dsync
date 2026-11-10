package runnerLocal

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/internal/app/options"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
)

const orderFlow = "cdp/dsync-prod-order-v2"

type connectorReportingID struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	id string
}

func (c connectorReportingID) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     c.id,
		DbType: "MongoDB",
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON}},
			Sink:   &adiomv1.Capabilities_Sink{SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON}},
		},
	}), nil
}

func fullSync(flowName string) RunnerLocalSettings {
	return RunnerLocalSettings{FlowName: flowName, NsFromString: []string{"cdp.order"}, SyncMode: iface.SyncModeFull}
}

// srcConnID and dstConnID stand in for what real connectors report: a hash of their connection string.
func resolveFlow(t *testing.T, srcConnID, dstConnID string, settings RunnerLocalSettings) (flowID iface.FlowID, src, dst iface.ConnectorID) {
	t.Helper()
	settings.Src = options.ConfiguredConnector{Local: connectorReportingID{id: srcConnID}}
	settings.Dst = options.ConfiguredConnector{Local: connectorReportingID{id: dstConnID}}
	// Cleanup makes Run return right after the flow ID is resolved, without starting the flow.
	settings.CleanupRequestedFlag = true
	r := NewRunnerLocal(settings)
	require.NoError(t, r.Setup(context.Background()))
	t.Cleanup(r.Teardown)
	require.NoError(t, r.Run())
	for _, details := range r.coord.GetConnectors() {
		switch details.Desc {
		case sourceName:
			src = details.Id
		case destinationName:
			dst = details.Id
		}
	}
	return r.activeFlowID, src, dst
}

func TestFlowNameKeepsFlowIDWhenConnectionStringsChange(t *testing.T) {
	before, _, _ := resolveFlow(t, "5d4a6c1f", "9b8e7d2a", fullSync(orderFlow))
	after, _, _ := resolveFlow(t, "c07f11e2", "34ab90d5", fullSync(orderFlow))

	require.Equal(t, before, after)
}

func TestWithoutFlowNameIDsAreUnchanged(t *testing.T) {
	flowID, src, dst := resolveFlow(t, "5d4a6c1f", "9b8e7d2a", fullSync(""))

	require.Equal(t, iface.ConnectorID("5d4a6c1f-Source"), src)
	require.Equal(t, iface.ConnectorID("9b8e7d2a-Destination"), dst)
	// Recorded on the code before --flow-name existed: any other value means
	// flows started without the flag no longer find their saved state.
	require.Equal(t, iface.FlowID("27eb88b383cddf9e"), flowID)
}

func TestDifferentFlowNamesAreDifferentFlows(t *testing.T) {
	order, _, _ := resolveFlow(t, "5d4a6c1f", "9b8e7d2a", fullSync(orderFlow))
	customer, _, _ := resolveFlow(t, "5d4a6c1f", "9b8e7d2a", fullSync("cdp/dsync-prod-customer"))

	require.NotEqual(t, order, customer)
}

func TestFlowNameGivesSourceAndDestinationDistinctIDs(t *testing.T) {
	_, src, dst := resolveFlow(t, "5d4a6c1f", "9b8e7d2a", fullSync(orderFlow))

	require.Equal(t, iface.ConnectorID("cdp/dsync-prod-order-v2-Source"), src)
	require.Equal(t, iface.ConnectorID("cdp/dsync-prod-order-v2-Destination"), dst)
}

func TestFlowNameDoesNotMergeOtherNamespacesOrModes(t *testing.T) {
	otherNamespace := fullSync(orderFlow)
	otherNamespace.NsFromString = []string{"cdp.customer"}
	otherMode := fullSync(orderFlow)
	otherMode.SyncMode = iface.SyncModeCDC

	base, _, _ := resolveFlow(t, "5d4a6c1f", "9b8e7d2a", fullSync(orderFlow))
	withOtherNamespace, _, _ := resolveFlow(t, "5d4a6c1f", "9b8e7d2a", otherNamespace)
	withOtherMode, _, _ := resolveFlow(t, "5d4a6c1f", "9b8e7d2a", otherMode)

	require.NotEqual(t, base, withOtherNamespace)
	require.NotEqual(t, base, withOtherMode)
}

func TestReverseUnderTheSameFlowNameIsASeparateFlow(t *testing.T) {
	forward := fullSync(orderFlow)
	forward.SyncMode = iface.SyncModeCDC
	reverse := forward
	reverse.ReverseRequestedFlag = true

	forwardID, _, _ := resolveFlow(t, "5d4a6c1f", "9b8e7d2a", forward)
	reverseID, _, _ := resolveFlow(t, "5d4a6c1f", "9b8e7d2a", reverse)

	require.NotEqual(t, forwardID, reverseID)
}
